"""Disposable local coordination snapshots. Never deserialize executable code.

Snapshots accelerate repeated local replay, not the event append path. A pinned
SQLite read transaction supplies both the cached prefix and its replay suffix.
"""
from __future__ import annotations

import hashlib
import json
import inspect
import sqlite3
import sys
from types import CodeType
from dataclasses import MISSING, dataclass, fields, is_dataclass
from functools import lru_cache
from pathlib import Path

from logplane import projection
from logplane import scheduling
from logplane import topics
FORMAT_VERSION = 1
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
RECORDS = {cls.__name__: cls for cls in (
    projection.WorkerRecord, projection.TaskRecord, projection.WorkflowControlRecord,
    projection.AgentPolicyRecord, projection.AssignmentAccountingRecord,
)}


@lru_cache(maxsize=1)
def reducer_fingerprint() -> str:
    # Hash loaded implementation, NOT source files: editing a checkout while
    # an old PM is running must not let it label old state as the new reducer.
    digest = hashlib.sha256()
    digest.update(str(FORMAT_VERSION).encode())
    for module in (sys.modules[__name__], projection, scheduling, topics):
        for name, value in sorted(vars(module).items()):
            if inspect.isfunction(value) and value.__module__ == module.__name__:
                digest.update(name.encode())
                digest.update(json.dumps(_code_identity(value.__code__), sort_keys=True).encode())
                digest.update(_defaults_identity(value).encode())
            elif inspect.isclass(value) and value.__module__ == module.__name__:
                if is_dataclass(value):
                    for field in fields(value):
                        digest.update(f"{name}.{field.name}:{field.type}".encode())
                        if field.default is not MISSING:
                            digest.update(json.dumps(_pack(field.default), sort_keys=True).encode())
                        if field.default_factory is not MISSING:
                            digest.update(field.default_factory.__qualname__.encode())
                for method, function in sorted(vars(value).items()):
                    if inspect.isfunction(function):
                        digest.update(f"{name}.{method}".encode())
                        digest.update(json.dumps(_code_identity(function.__code__), sort_keys=True).encode())
                        digest.update(_defaults_identity(function).encode())
            elif (
                isinstance(value, dict)
                and value
                and all(inspect.isfunction(item) for item in value.values())
            ):
                # Dispatch tables: which topic runs which handler is part of the
                # reducer, even though each handler's code is hashed on its own.
                digest.update(name.encode())
                for key, function in sorted(value.items()):
                    digest.update(f"{key}={function.__module__}.{function.__qualname__}".encode())
            elif name.isupper() and isinstance(value, (str, int, float, tuple, frozenset, list, set, dict)):
                # Canonicalize policy/topic constants, including unordered sets.
                try:
                    encoded = _pack(frozenset(value) if isinstance(value, set) else value)
                    digest.update(json.dumps(encoded, sort_keys=True).encode())
                except ValueError:
                    pass  # Class registries are covered by their loaded methods.
    return digest.hexdigest()


def _defaults_identity(function) -> str:
    # Default argument values live on the function, not in its code object, so a
    # changed default must be hashed separately or old snapshots would replay
    # under the new behavior.
    return repr((function.__defaults__, sorted((function.__kwdefaults__ or {}).items())))


def _code_identity(code):
    # Exclude filenames, line tables, and interpreter string-interning details;
    # these can differ between processes without changing reducer behavior.
    def constant(value):
        if isinstance(value, CodeType):
            return _code_identity(value)
        if isinstance(value, (tuple, frozenset)):
            items = [constant(item) for item in value]
            return sorted(items, key=repr) if isinstance(value, frozenset) else items
        if value is None or type(value) in (str, int, float, bool):
            return value
        return repr(value)
    return {"code": code.co_code.hex(), "constants": [constant(v) for v in code.co_consts],
            "names": code.co_names, "variables": code.co_varnames,
            "free": code.co_freevars, "cells": code.co_cellvars,
            "arguments": [code.co_argcount, code.co_posonlyargcount, code.co_kwonlyargcount],
            "flags": code.co_flags, "exceptions": getattr(code, "co_exceptiontable", b"").hex()}


def _pack(value):
    if value is None or type(value) in (str, int, float, bool):
        return value
    if is_dataclass(value) and type(value).__name__ in RECORDS:
        return {"record": type(value).__name__,
                "fields": {f.name: _pack(getattr(value, f.name)) for f in fields(value)}}
    if isinstance(value, dict):
        return {"dict": [[_pack(k), _pack(v)] for k, v in value.items()]}
    for cls, tag in ((list, "list"), (tuple, "tuple"), (frozenset, "frozenset")):
        if isinstance(value, cls):
            items = sorted(value) if cls is frozenset else value
            return {tag: [_pack(item) for item in items]}
    raise ValueError(f"unsupported snapshot type: {type(value).__name__}")


def _unpack(value):
    if value is None or type(value) in (str, int, float, bool):
        return value
    if not isinstance(value, dict):
        raise ValueError("invalid snapshot value")
    if set(value) == {"record", "fields"}:
        cls = RECORDS.get(value["record"])
        data = value["fields"]
        if cls is None or not isinstance(data, dict) or set(data) != {f.name for f in fields(cls)}:
            raise ValueError("incompatible snapshot record")
        return cls(**{key: _unpack(item) for key, item in data.items()})
    if len(value) != 1:
        raise ValueError("invalid snapshot container")
    tag, items = next(iter(value.items()))
    if not isinstance(items, list):
        raise ValueError("invalid snapshot items")
    if tag == "dict":
        result = {}
        for pair in items:
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError("invalid snapshot mapping")
            key, item = map(_unpack, pair)
            if key in result:
                raise ValueError("duplicate snapshot key")
            result[key] = item
        return result
    constructors = {"list": list, "tuple": tuple, "frozenset": frozenset}
    if tag not in constructors:
        raise ValueError("unknown snapshot container")
    return constructors[tag](_unpack(item) for item in items)


def encode_state(state: projection.PMState) -> str:
    return json.dumps(_pack(vars(state)), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _reject_constant(name: str):
    # encode_state refuses NaN and Infinity; a snapshot containing them is corrupt.
    raise ValueError(f"invalid snapshot constant: {name}")


def decode_state(data: str) -> projection.PMState:
    state = projection.PMState()
    values = _unpack(json.loads(data, parse_constant=_reject_constant))
    if not isinstance(values, dict) or set(values) != set(vars(state)):
        raise ValueError("incompatible snapshot state")
    mappings = {"workers": projection.WorkerRecord, "tasks": projection.TaskRecord,
                "workflows": projection.WorkflowControlRecord,
                "agent_policies": projection.AgentPolicyRecord,
                "assignment_accounting": projection.AssignmentAccountingRecord}
    for name, cls in mappings.items():
        if not isinstance(values[name], dict) or not all(type(v) is cls for v in values[name].values()):
            raise ValueError("invalid snapshot record map")
    vars(state).update(values)
    return state


def _connect(path: Path) -> sqlite3.Connection:
    # mode=rw avoids silently creating a different/empty database on path typos.
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=rw", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def database_identity(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT value FROM bus_metadata WHERE name='database_id'").fetchone()
    if row is None:
        raise ValueError("database identity missing; start the upgraded bus first")
    return row[0]


def _anchor(conn, event_id):
    if event_id == 0:
        return hashlib.sha256(b"empty event log").hexdigest()
    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    if row is None:
        raise ValueError("snapshot event anchor missing")
    return event_anchor(dict(row))


def event_anchor(event: dict) -> str:
    normalized = dict(event)
    for field in ("payload", "producer"):
        if isinstance(normalized.get(field), str):
            normalized[field] = json.loads(normalized[field])
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass
class ReplayResult:
    state: projection.PMState
    last_event_id: int
    database_id: str
    anchor: str
    snapshot_event_id: int | None
    replayed_events: int
    cache_note: str


def load_projection(path: Path, *, use_snapshot: bool = True,
                    through_id: int | None = None) -> ReplayResult:
    if through_id is not None and (type(through_id) is not int or through_id < 0):
        raise ValueError("through_id must be nonnegative")
    conn = _connect(path)
    try:
        conn.execute("BEGIN")
        identity = database_identity(conn)
        head = conn.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0]
        if through_id is not None:
            if through_id > head:
                raise ValueError("requested cursor is beyond event history")
            head = through_id
        state, cursor, cached = projection.PMState(), 0, None
        note = "snapshot disabled" if not use_snapshot else "snapshot absent"
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='coordination_snapshot'").fetchone()
        if use_snapshot and exists:
            try:
                row = conn.execute("SELECT * FROM coordination_snapshot WHERE slot=1").fetchone()
                if row is not None:
                    if not isinstance(row["envelope"], str) or not isinstance(row["sha256"], str):
                        raise ValueError("invalid snapshot storage types")
                    payload = row["envelope"].encode()
                    if len(payload) > MAX_SNAPSHOT_BYTES:
                        raise ValueError("snapshot too large")
                    if hashlib.sha256(payload).hexdigest() != row["sha256"]:
                        raise ValueError("snapshot checksum mismatch")
                    envelope = json.loads(row["envelope"])
                    if envelope["format"] != FORMAT_VERSION or envelope["reducer"] != reducer_fingerprint():
                        raise ValueError("snapshot implementation changed")
                    if envelope["database_id"] != identity:
                        raise ValueError("snapshot belongs to another database")
                    cached_head = envelope["head"]
                    if type(cached_head) is not int or not 0 <= cached_head <= head:
                        raise ValueError("snapshot beyond requested history")
                    if envelope["anchor"] != _anchor(conn, cached_head):
                        raise ValueError("snapshot anchor changed")
                    state = decode_state(envelope["state"])
                    cursor = cached = cached_head
                    note = "snapshot loaded"
            except (ValueError, TypeError, KeyError, IndexError, RecursionError, sqlite3.Error) as exc:
                state, cursor, cached = projection.PMState(), 0, None
                note = f"snapshot ignored: {exc}"
        count = 0
        topic_list = projection.PROJECTION_TOPICS
        rows = conn.execute(
            f"SELECT * FROM events WHERE id>? AND id<=? AND topic IN ({','.join('?' for _ in topic_list)}) ORDER BY id",
            (cursor, head, *topic_list),
        )
        for row in rows:
            event = dict(row)
            event["payload"] = json.loads(event["payload"])
            if event["producer"] is not None:
                event["producer"] = json.loads(event["producer"])
            projection.apply_event(state, event)
            count += 1
        return ReplayResult(state, head, identity, _anchor(conn, head), cached, count, note)
    finally:
        conn.close()


def save_projection(path: Path, result: ReplayResult) -> bool:
    """Save a state returned by load_projection, before callers mutate it."""
    envelope = json.dumps({"format": FORMAT_VERSION, "reducer": reducer_fingerprint(),
                          "database_id": result.database_id, "head": result.last_event_id,
                          "anchor": result.anchor, "state": encode_state(result.state)},
                         sort_keys=True, separators=(",", ":"))
    if len(envelope.encode()) > MAX_SNAPSHOT_BYTES:
        return False
    conn = _connect(path)
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            if database_identity(conn) != result.database_id or _anchor(conn, result.last_event_id) != result.anchor:
                raise ValueError("database changed while building snapshot")
            conn.execute("CREATE TABLE IF NOT EXISTS coordination_snapshot ("
                         "slot INTEGER PRIMARY KEY CHECK(slot=1), envelope TEXT NOT NULL, sha256 TEXT NOT NULL)")
            conn.execute("INSERT OR REPLACE INTO coordination_snapshot VALUES (1,?,?)",
                         (envelope, hashlib.sha256(envelope.encode()).hexdigest()))
        return True
    finally:
        conn.close()


def clear_projection(path: Path) -> None:
    conn = _connect(path)
    try:
        with conn:
            conn.execute("DROP TABLE IF EXISTS coordination_snapshot")
    finally:
        conn.close()
