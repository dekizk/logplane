"""
logplane: an append-only event bus for local multi-agent orchestration.

Events are immutable SQLite rows. Consumers rebuild state by replaying the log,
then follow the same ordered stream over SSE. Versioned contracts protect known
orchestration events while unknown topics remain available to extensions.

Run: python -m uvicorn bus:app --port 8765
"""

import asyncio
import contextlib
import json
import os
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

# The validator and its exception stay importable from logplane.bus, which is
# where callers found them before validation moved to its own module.
from logplane.event_validation import (
    CURRENT_SCHEMA_VERSION,
    MAX_CORRELATION_ID_LENGTH,
    EventValidationError,
    _is_nonnegative_int,
    _is_nonnegative_number,
    _is_positive_int,
    _validate_correlation_id,
    validate_event,
)

from logplane.legacy import check_legacy_environment
from logplane.scheduling import DEFAULT_TASK_PRIORITY
from logplane.version import VERSION

DB_PATH = Path(os.environ.get("LOGPLANE_DB_PATH", Path.cwd() / "events.db"))


def _read_default_max_retries() -> int:
    raw_value = os.environ.get("LOGPLANE_MAX_RETRIES", "2")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            "LOGPLANE_MAX_RETRIES must be a non-negative integer"
        ) from exc
    if value < 0:
        raise RuntimeError("LOGPLANE_MAX_RETRIES must be a non-negative integer")
    return value


DEFAULT_MAX_RETRIES = _read_default_max_retries()

# Refuse to start with pre-0.13 AGENT_BUS_* settings: an ignored
# AGENT_BUS_TOKEN would otherwise leave the bus unauthenticated.
check_legacy_environment()

# Optional perimeter auth: when LOGPLANE_TOKEN is set, every data route
# requires "Authorization: Bearer <token>". Actor strings are still
# self-reported — this authenticates clients, not identities.
def _read_api_token() -> Optional[str]:
    value = os.environ.get("LOGPLANE_TOKEN")
    if value is not None and not value.strip():
        # An empty value usually means an unexpanded variable or a typo. Treating
        # it as "no auth" would silently start an unauthenticated bus.
        raise RuntimeError(
            "LOGPLANE_TOKEN is set but empty; set a token, or unset it to run "
            "without authentication"
        )
    return value


API_TOKEN = _read_api_token()


async def require_token(request: Request) -> None:
    if not API_TOKEN:
        return
    # Compare bytes: compare_digest rejects non-ASCII str with TypeError, which
    # would turn a bad header into a 500. Starlette decodes headers as latin-1,
    # so encoding back to latin-1 recovers the bytes the client sent.
    supplied = request.headers.get("authorization", "").encode("latin-1", "replace")
    expected = f"Bearer {API_TOKEN}".encode("utf-8")
    if not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")

class IdempotencyConflict(ValueError):
    """An idempotency key was reused for a different logical request."""


def _validate_query_correlation_id(correlation_id: Optional[str]) -> None:
    try:
        _validate_correlation_id(correlation_id)
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


# ---------------------------------------------------------------- storage


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextlib.contextmanager
def db():
    """Connection that both commits the transaction AND closes the handle.

    sqlite3's own `with conn:` only commits/rolls back — it does NOT close,
    so using `with _connect() as conn:` alone would leak a connection (and
    keep the WAL/-shm files open) on every call.
    """
    conn = _connect()
    try:
        with conn:  # transaction scope (commit/rollback)
            yield conn
    finally:
        conn.close()  # lifetime scope


def init_db() -> None:
    with db() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        # Serialize schema/backfill work with publishers; no partially built
        # identity projection may become visible after a crash or concurrent init.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("CREATE TABLE IF NOT EXISTS bus_metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT OR IGNORE INTO bus_metadata VALUES ('database_id',?)", (uuid.uuid4().hex,))
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        REAL    NOT NULL,
                topic     TEXT    NOT NULL,
                actor     TEXT    NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                idempotency_key TEXT,
                caused_by INTEGER,
                correlation_id TEXT,
                producer  TEXT,
                payload   TEXT    NOT NULL
            )
            """
        )
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
        if "idempotency_key" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN idempotency_key TEXT")
        if "schema_version" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
            )
        if "correlation_id" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN correlation_id TEXT")
        if "producer" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN producer TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS counters (
                name  TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, id)")
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_identity'"
        ).fetchone()
        _create_task_identity(conn)
        if exists is None:
            _backfill_task_identity(conn)
        _advance_task_counter(conn)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_correlation_id
            ON events(correlation_id, id)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_events_actor_idempotency
            ON events(actor, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )
        # This is a rebuildable uniqueness projection over immutable adoption
        # events. Unlike ordinary idempotency keys, an external origin is
        # process/actor independent: two misconfigured bridge actor names must
        # not be able to create two orchestration owners for the same work.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_origin_claims (
                system   TEXT NOT NULL,
                task_ref TEXT NOT NULL,
                event_id INTEGER,
                PRIMARY KEY (system, task_ref)
            )
            """
        )
        for row in conn.execute(
            """
            SELECT id, payload FROM events
            WHERE topic IN ('task.created', 'integration.task_observed')
            ORDER BY id
            """
        ):
            try:
                origin = json.loads(row["payload"]).get("external_origin")
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
            if not isinstance(origin, dict):
                continue
            system = origin.get("system")
            task_ref = origin.get("task_ref")
            if isinstance(system, str) and isinstance(task_ref, str):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO external_origin_claims
                        (system, task_ref, event_id)
                    VALUES (?, ?, ?)
                    """,
                    (system, task_ref, row["id"]),
                )
        # One immutable task can name at most one direct replacement. This
        # rebuildable uniqueness projection makes that invariant atomic across
        # actors and concurrent publishers; later changes supersede the
        # replacement task rather than branching the old intent.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_supersession_claims (
                task_id  INTEGER PRIMARY KEY,
                event_id INTEGER
            )
            """
        )
        for row in conn.execute(
            "SELECT id, payload FROM events WHERE topic = 'task.created' ORDER BY id"
        ):
            try:
                supersedes_task_id = json.loads(row["payload"]).get(
                    "supersedes_task_id"
                )
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
            if _is_positive_int(supersedes_task_id):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO task_supersession_claims
                        (task_id, event_id)
                    VALUES (?, ?)
                    """,
                    (supersedes_task_id, row["id"]),
                )


def _create_task_identity(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS task_identity ("
        "task_id INTEGER PRIMARY KEY, created_event_id INTEGER NOT NULL UNIQUE)"
    )


def _backfill_task_identity(conn: sqlite3.Connection) -> None:
    # First valid creation wins, matching historical task identity semantics.
    # This O(task history) scan is reserved for migration/explicit recovery.
    for row in conn.execute(
        "SELECT id, payload FROM events WHERE topic='task.created' ORDER BY id"
    ):
        try:
            payload = json.loads(row["payload"])
        except (json.JSONDecodeError, TypeError):
            continue
        task_id = payload.get("task_id") if isinstance(payload, dict) else None
        if _is_positive_int(task_id):
            conn.execute(
                "INSERT OR IGNORE INTO task_identity VALUES (?, ?)",
                (task_id, row["id"]),
            )


def _advance_task_counter(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO counters(name,value) VALUES ('task_id',"
        "(SELECT COALESCE(MAX(task_id),0) FROM task_identity)) "
        "ON CONFLICT(name) DO UPDATE SET value=MAX(value,excluded.value)"
    )


def rebuild_task_index() -> int:
    """Atomically replace the disposable identity index, never event history.

    Run against an initialized database. Writers wait for the transaction;
    readers retain their previous consistent view. Failures roll back the rebuild.
    """
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        _create_task_identity(conn)
        conn.execute("DELETE FROM task_identity")
        _backfill_task_identity(conn)
        _advance_task_counter(conn)
        return conn.execute("SELECT COUNT(*) FROM task_identity").fetchone()[0]


def next_task_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "UPDATE counters SET value = value + 1 WHERE name = 'task_id' RETURNING value"
    ).fetchone()
    if row is None:
        conn.execute("INSERT INTO counters (name, value) VALUES ('task_id', 1)")
        return 1
    return int(row["value"])


def row_to_dict(row: sqlite3.Row) -> dict:
    event = dict(row)
    event["payload"] = json.loads(event["payload"])
    if event.get("producer") is not None:
        event["producer"] = json.loads(event["producer"])
    event.setdefault("schema_version", 1)
    event.setdefault("correlation_id", None)
    event.setdefault("producer", None)
    return event


def _find_task_created(
    conn: sqlite3.Connection,
    task_id: int,
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT events.* FROM task_identity AS identity "
        "JOIN events ON events.id=identity.created_event_id "
        "WHERE identity.task_id=?", (task_id,),
    ).fetchone()


def _resolve_correlation_id(
    conn: sqlite3.Connection,
    *,
    topic: str,
    actor: str,
    caused_by: Optional[int],
    correlation_id: Optional[str],
    payload: dict,
    producer: Optional[dict],
) -> Optional[str]:
    """Resolve a new event's workflow identity inside its append transaction."""
    resolved = correlation_id
    if caused_by is not None:
        parent = conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (caused_by,),
        ).fetchone()
        if parent is None:
            raise EventValidationError(f"caused_by event {caused_by} does not exist")
        parent_correlation_id = parent["correlation_id"]
        if (
            parent_correlation_id is not None
            and resolved is not None
            and resolved != parent_correlation_id
        ):
            raise EventValidationError(
                "correlation_id conflicts with the caused_by event"
            )
        resolved = parent_correlation_id or resolved

        if topic == "workflow.usage_recorded":
            try:
                parent_payload = json.loads(parent["payload"])
                parent_producer = (
                    json.loads(parent["producer"])
                    if parent["producer"] is not None
                    else None
                )
            except (json.JSONDecodeError, TypeError) as exc:
                raise EventValidationError(
                    "usage accounting parent is malformed"
                ) from exc
            if parent["topic"] not in {
                "telemetry.model.completed",
                "telemetry.model.failed",
            }:
                raise EventValidationError(
                    "workflow.usage_recorded must follow terminal model telemetry"
                )
            for field in (
                "task_id",
                "assignment_id",
                "worker_instance_id",
                "invocation_id",
            ):
                if payload.get(field) != parent_payload.get(field):
                    raise EventValidationError(
                        f"usage accounting {field} conflicts with telemetry"
                    )
            if parent["actor"] != actor or parent_producer != producer:
                raise EventValidationError(
                    "usage accounting identity conflicts with telemetry"
                )
            usage = parent_payload.get("usage")
            usage = usage if isinstance(usage, dict) else {}
            expected_tokens = usage.get("total_tokens")
            if not _is_nonnegative_int(expected_tokens):
                input_tokens = usage.get("input_tokens")
                output_tokens = usage.get("output_tokens")
                expected_tokens = (
                    input_tokens + output_tokens
                    if _is_nonnegative_int(input_tokens)
                    and _is_nonnegative_int(output_tokens)
                    else None
                )
            expected_cost = usage.get(
                "cost_usd", usage.get("estimated_cost_usd")
            )
            if not _is_nonnegative_number(expected_cost):
                expected_cost = None
            if (
                payload.get("tokens") != expected_tokens
                or payload.get("cost_usd") != expected_cost
            ):
                raise EventValidationError(
                    "usage accounting values conflict with telemetry"
                )

    if topic == "task.created":
        supersedes_task_id = payload.get("supersedes_task_id")
        if supersedes_task_id is not None:
            superseded = _find_task_created(conn, supersedes_task_id)
            if superseded is None:
                raise EventValidationError(
                    f"task {supersedes_task_id} does not exist"
                )
            superseded_correlation_id = superseded["correlation_id"]
            if superseded_correlation_id is None:
                raise EventValidationError(
                    f"task {supersedes_task_id} has no workflow identity"
                )
            if resolved is not None and resolved != superseded_correlation_id:
                raise EventValidationError(
                    "correlation_id conflicts with the superseded task"
                )
            resolved = superseded_correlation_id
        for dependency_task_id in payload.get("depends_on", []):
            dependency = _find_task_created(conn, dependency_task_id)
            if dependency is None:
                raise EventValidationError(
                    f"dependency task {dependency_task_id} does not exist"
                )
            dependency_correlation_id = dependency["correlation_id"]
            if dependency_correlation_id is None:
                raise EventValidationError(
                    f"dependency task {dependency_task_id} has no workflow identity"
                )
            if resolved is not None and resolved != dependency_correlation_id:
                raise EventValidationError(
                    "all dependencies must belong to the same correlation_id"
                )
            resolved = dependency_correlation_id

    if topic in {
        "task.cancel_requested",
        "task.pause_requested",
        "task.resume_requested",
    }:
        task = _find_task_created(conn, payload["task_id"])
        if task is None:
            raise EventValidationError(
                f"task {payload['task_id']} does not exist"
            )
        task_correlation_id = task["correlation_id"]
        if (
            task_correlation_id is not None
            and resolved is not None
            and resolved != task_correlation_id
        ):
            raise EventValidationError(
                "correlation_id conflicts with the target task"
            )
        resolved = task_correlation_id or resolved

    if topic.startswith("workflow."):
        if resolved is None:
            raise EventValidationError(f"{topic} requires correlation_id")
        exists = conn.execute(
            "SELECT 1 FROM events WHERE topic = 'task.created' AND correlation_id = ? LIMIT 1",
            (resolved,),
        ).fetchone()
        if exists is None:
            raise EventValidationError(
                f"workflow {resolved!r} does not exist"
            )

    if resolved is not None:
        return resolved
    if topic == "task.created":
        return uuid.uuid4().hex
    return None


def _assert_idempotent_match(
    row: sqlite3.Row,
    *,
    topic: str,
    payload: dict,
    caused_by: Optional[int],
    schema_version: int,
    correlation_id: Optional[str],
    producer: Optional[dict],
) -> None:
    existing = row_to_dict(row)
    stored_payload = existing["payload"]
    if topic == "task.created":
        stored_payload = dict(stored_payload)
        if "task_id" not in payload:
            # task_id is a server-generated response field, not part of the
            # caller's logical idempotent request.
            stored_payload.pop("task_id", None)
        if "retry_policy" not in payload:
            # The server materializes its current default into new task events.
            # Omitting it remains the same logical request on a retry.
            stored_payload.pop("retry_policy", None)
        if "ownership" not in payload:
            # Controlled ownership is a server-materialized default.
            stored_payload.pop("ownership", None)
        if "depends_on" not in payload:
            stored_payload.pop("depends_on", None)
        if "priority" not in payload:
            # Normal priority is a server-materialized scheduling default.
            stored_payload.pop("priority", None)
    # Omitting correlation_id delegates generation/inheritance to the server.
    # On a retry, the stored value is therefore the effective requested value.
    effective_correlation_id = (
        existing["correlation_id"] if correlation_id is None else correlation_id
    )
    requested = (
        topic,
        payload,
        caused_by,
        schema_version,
        effective_correlation_id,
        producer,
    )
    stored = (
        existing["topic"],
        stored_payload,
        existing["caused_by"],
        existing["schema_version"],
        existing["correlation_id"],
        existing["producer"],
    )
    if stored != requested:
        raise IdempotencyConflict(
            "idempotency key is already attached to a different event"
        )


def _recover_concurrent_idempotent_claim(
    conn: sqlite3.Connection,
    *,
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int],
    idempotency_key: Optional[str],
    schema_version: int,
    correlation_id: Optional[str],
    producer: Optional[dict],
) -> Optional[dict]:
    """Recover when a claim race lost to the same logical request."""
    if idempotency_key is None:
        return None
    # Release the failed write transaction before reading the winning event.
    conn.rollback()
    row = conn.execute(
        "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
        (actor, idempotency_key),
    ).fetchone()
    if row is None:
        return None
    _assert_idempotent_match(
        row,
        topic=topic,
        payload=payload,
        caused_by=caused_by,
        schema_version=schema_version,
        correlation_id=correlation_id,
        producer=producer,
    )
    return row_to_dict(row)


def append_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int] = None,
    idempotency_key: Optional[str] = None,
    schema_version: int = CURRENT_SCHEMA_VERSION,
    correlation_id: Optional[str] = None,
    producer: Optional[dict] = None,
) -> dict:
    validate_event(
        topic,
        actor,
        payload,
        caused_by,
        idempotency_key,
        schema_version,
        correlation_id,
        producer,
    )
    requested_payload = dict(payload)
    with db() as conn:
        # Identity checks and all derived claims must observe the same serialized
        # write transaction as append (including concurrent explicit task IDs).
        conn.execute("BEGIN IMMEDIATE")
        if idempotency_key is not None:
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is not None:
                _assert_idempotent_match(
                    row,
                    topic=topic,
                    payload=requested_payload,
                    caused_by=caused_by,
                    schema_version=schema_version,
                    correlation_id=correlation_id,
                    producer=producer,
                )
                return row_to_dict(row)

        resolved_correlation_id = _resolve_correlation_id(
            conn,
            topic=topic,
            actor=actor,
            caused_by=caused_by,
            correlation_id=correlation_id,
            payload=requested_payload,
            producer=producer,
        )
        payload = dict(requested_payload)
        origin_claim = None
        supersession_claim = None
        if topic in {"task.created", "integration.task_observed"}:
            origin = payload.get("external_origin")
            if isinstance(origin, dict):
                origin_claim = (origin["system"], origin["task_ref"])
                try:
                    conn.execute(
                        """
                        INSERT INTO external_origin_claims
                            (system, task_ref, event_id)
                        VALUES (?, ?, NULL)
                        """,
                        origin_claim,
                    )
                except sqlite3.IntegrityError as exc:
                    recovered = _recover_concurrent_idempotent_claim(
                        conn,
                        topic=topic,
                        actor=actor,
                        payload=requested_payload,
                        caused_by=caused_by,
                        idempotency_key=idempotency_key,
                        schema_version=schema_version,
                        correlation_id=correlation_id,
                        producer=producer,
                    )
                    if recovered is not None:
                        return recovered
                    existing = conn.execute(
                        """
                        SELECT event_id FROM external_origin_claims
                        WHERE system = ? AND task_ref = ?
                        """,
                        origin_claim,
                    ).fetchone()
                    event_label = (
                        f" at event #{existing['event_id']}"
                        if existing is not None and existing["event_id"] is not None
                        else ""
                    )
                    raise IdempotencyConflict(
                        "external origin "
                        f"{origin_claim[0]!r}/{origin_claim[1]!r} is already claimed"
                        f"{event_label}; refusing possible dual ownership"
                    ) from exc
        if topic == "task.created":
            supersedes_task_id = payload.get("supersedes_task_id")
            if supersedes_task_id is not None:
                supersession_claim = supersedes_task_id
                try:
                    conn.execute(
                        """
                        INSERT INTO task_supersession_claims (task_id, event_id)
                        VALUES (?, NULL)
                        """,
                        (supersession_claim,),
                    )
                except sqlite3.IntegrityError as exc:
                    recovered = _recover_concurrent_idempotent_claim(
                        conn,
                        topic=topic,
                        actor=actor,
                        payload=requested_payload,
                        caused_by=caused_by,
                        idempotency_key=idempotency_key,
                        schema_version=schema_version,
                        correlation_id=correlation_id,
                        producer=producer,
                    )
                    if recovered is not None:
                        return recovered
                    existing = conn.execute(
                        """
                        SELECT event_id FROM task_supersession_claims
                        WHERE task_id = ?
                        """,
                        (supersession_claim,),
                    ).fetchone()
                    event_label = (
                        f" at event #{existing['event_id']}"
                        if existing is not None and existing["event_id"] is not None
                        else ""
                    )
                    raise EventValidationError(
                        f"task {supersession_claim} already has a replacement"
                        f"{event_label}"
                    ) from exc
            payload.setdefault(
                "retry_policy",
                {"max_retries": DEFAULT_MAX_RETRIES},
            )
            payload.setdefault(
                "ownership",
                {"mode": "controlled", "owner": "agent-bus"},
            )
            payload.setdefault("depends_on", [])
            payload.setdefault("priority", DEFAULT_TASK_PRIORITY)
            if "task_id" not in payload:
                payload["task_id"] = next_task_id(conn)
            elif _is_positive_int(payload["task_id"]):
                if _find_task_created(conn, payload["task_id"]) is not None:
                    raise EventValidationError(
                        f"task_id {payload['task_id']} already exists"
                    )
                conn.execute(
                    "UPDATE counters SET value = max(value, ?) WHERE name = 'task_id'",
                    (payload["task_id"],),
                )
        if topic == "task.retry_requested":
            task_id = payload["task_id"]
            replacement = conn.execute(
                "SELECT event_id FROM task_supersession_claims WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if replacement is not None:
                event_label = (
                    f" at event #{replacement['event_id']}"
                    if replacement["event_id"] is not None
                    else ""
                )
                raise EventValidationError(
                    f"task {task_id} has already been superseded{event_label} "
                    "and cannot be retried"
                )

        try:
            cur = conn.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, idempotency_key, caused_by,
                     correlation_id, producer, payload)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(),
                    topic,
                    actor,
                    schema_version,
                    idempotency_key,
                    caused_by,
                    resolved_correlation_id,
                    json.dumps(producer, sort_keys=True, separators=(",", ":"))
                    if producer is not None
                    else None,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )
        except sqlite3.IntegrityError:
            if idempotency_key is None:
                raise
            # Undo any counter increment performed before a racing idempotent
            # insert lost the unique-index race.
            conn.rollback()
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is None:
                raise
            _assert_idempotent_match(
                row,
                topic=topic,
                payload=requested_payload,
                caused_by=caused_by,
                schema_version=schema_version,
                correlation_id=correlation_id,
                producer=producer,
            )
            return row_to_dict(row)
        row = conn.execute("SELECT * FROM events WHERE id = ?", (cur.lastrowid,)).fetchone()
        if topic == "task.created":
            conn.execute(
                "INSERT INTO task_identity(task_id,created_event_id) VALUES (?,?)",
                (payload["task_id"], cur.lastrowid),
            )
        if origin_claim is not None:
            conn.execute(
                """
                UPDATE external_origin_claims SET event_id = ?
                WHERE system = ? AND task_ref = ?
                """,
                (cur.lastrowid, *origin_claim),
            )
        if supersession_claim is not None:
            conn.execute(
                """
                UPDATE task_supersession_claims SET event_id = ?
                WHERE task_id = ?
                """,
                (cur.lastrowid, supersession_claim),
            )
    return row_to_dict(row)


def fetch_after(
    after_id: int,
    topics: Optional[list[str]],
    limit: int = 1000,
    correlation_id: Optional[str] = None,
) -> list[dict]:
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    if correlation_id is not None:
        q += " AND correlation_id = ?"
        args.append(correlation_id)
    q += " ORDER BY id LIMIT ?"
    args.append(limit)
    with db() as conn:
        return [row_to_dict(r) for r in conn.execute(q, args).fetchall()]


def fetch_event(event_id: int) -> Optional[dict]:
    with db() as conn:
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    return row_to_dict(row) if row is not None else None


def fetch_stream_window(
    after_id: int,
    topics: Optional[list[str]],
    limit: int = 500,
    correlation_id: Optional[str] = None,
) -> tuple[list[dict], int, bool]:
    """Read a bounded matching-id window without decoding unrelated events."""
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    if correlation_id is not None:
        q += " AND correlation_id = ?"
        args.append(correlation_id)
    q += " ORDER BY id LIMIT ?"
    args.append(limit)
    with db() as conn:
        rows = conn.execute(q, args).fetchall()
    events = [row_to_dict(row) for row in rows]
    scanned_to = events[-1]["id"] if events else after_id
    return events, scanned_to, len(events) == limit


# ---------------------------------------------------------------- app

new_event = asyncio.Condition()
event_generation = 0

# Open event streams wait for new events in short slices so they notice a
# departed client or a server shutdown within about a second, while idle
# keepalives keep their original 15-second cadence.
STREAM_POLL_SECONDS = 1.0
STREAM_KEEPALIVE_SECONDS = 15.0
_shutting_down = False


def begin_shutdown() -> None:
    """Ask open event streams to finish so the server can stop promptly.

    Called by `logplane serve` when it receives a stop signal. Clients see
    the stream end and reconnect with their normal backoff.
    """
    global _shutting_down
    _shutting_down = True


def _stream_should_stop_now() -> bool:
    return _shutting_down


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(init_db)
    yield


app = FastAPI(title="logplane", version=VERSION, lifespan=lifespan)


class PublishRequest(BaseModel):
    topic: str = Field(..., examples=["task.created"])
    actor: str = Field(..., examples=["pm"])
    payload: dict = Field(default_factory=dict)
    caused_by: Optional[int] = None
    idempotency_key: Optional[str] = None
    schema_version: int = CURRENT_SCHEMA_VERSION
    correlation_id: Optional[str] = None
    producer: Optional[dict] = None


@app.post("/events", dependencies=[Depends(require_token)])
async def publish(req: PublishRequest) -> dict:
    global event_generation
    try:
        event = await run_in_threadpool(
            append_event,
            req.topic,
            req.actor,
            req.payload,
            req.caused_by,
            req.idempotency_key,
            req.schema_version,
            req.correlation_id,
            req.producer,
        )
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    async with new_event:
        event_generation += 1
        new_event.notify_all()
    return event


@app.get("/events", dependencies=[Depends(require_token)])
async def query(
    response: Response,
    after_id: int = 0,
    topics: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=10_000),
    correlation_id: Optional[str] = Query(
        None, min_length=1, max_length=MAX_CORRELATION_ID_LENGTH
    ),
) -> list[dict]:
    _validate_query_correlation_id(correlation_id)
    topic_list = [topic.strip() for topic in topics.split(",") if topic.strip()] if topics else None
    events = await run_in_threadpool(
        fetch_after, after_id, topic_list, limit, correlation_id
    )
    # A full page means more events may exist; callers that want everything
    # should paginate (see BusClient.query_all) instead of trusting one page.
    response.headers["X-Page-Full"] = "1" if len(events) == limit else "0"
    return events


@app.get("/events/stream", dependencies=[Depends(require_token)])
async def stream(
    request: Request,
    from_id: int = 0,
    topics: Optional[str] = Query(None),
    correlation_id: Optional[str] = Query(
        None, min_length=1, max_length=MAX_CORRELATION_ID_LENGTH
    ),
):
    """Replay history after from_id in bounded windows, then follow live."""
    _validate_query_correlation_id(correlation_id)
    topic_list = [topic.strip() for topic in topics.split(",") if topic.strip()] if topics else None

    async def gen():
        last_id = from_id
        observed_generation = event_generation
        while True:
            if _stream_should_stop_now() or await request.is_disconnected():
                return
            events, scanned_to, full_window = await run_in_threadpool(
                fetch_stream_window, last_id, topic_list, 500, correlation_id
            )
            last_id = scanned_to
            for event in events:
                yield f"id: {event['id']}\ndata: {json.dumps(event)}\n\n"
            if full_window:
                continue
            # Wait for the next event without polling the database, checking
            # for a departed client or shutdown each slice. After a full
            # keepalive interval with no event, send a keepalive and re-check
            # history, exactly as the single 15-second wait used to.
            waited = 0.0
            while True:
                if _stream_should_stop_now() or await request.is_disconnected():
                    return
                try:
                    async with new_event:
                        if event_generation == observed_generation:
                            await asyncio.wait_for(
                                new_event.wait(), timeout=STREAM_POLL_SECONDS
                            )
                        observed_generation = event_generation
                    break
                except asyncio.TimeoutError:
                    waited += STREAM_POLL_SECONDS
                    if waited >= STREAM_KEEPALIVE_SECONDS:
                        yield ": keepalive\n\n"
                        break

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/events/{event_id}", dependencies=[Depends(require_token)])
async def get_event(event_id: int) -> dict:
    if event_id <= 0:
        raise HTTPException(status_code=422, detail="event_id must be positive")
    event = await run_in_threadpool(fetch_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event


@app.get("/health")
async def health(response: Response) -> dict:
    # Local launcher readiness, not an authentication credential. Only the bus
    # child receives this per-launch marker; ordinary `serve` needs no marker.
    startup_id = os.environ.get("LOGPLANE_STARTUP_ID")
    if startup_id:
        response.headers["X-Logplane-Startup-Id"] = startup_id
    def identity():
        with db() as conn:
            row = conn.execute("SELECT value, (SELECT COALESCE(MAX(id), 0) FROM events) "
                               "FROM bus_metadata WHERE name='database_id'").fetchone()
            return {"database_id": row[0], "last_event_id": row[1]}
    return {"ok": True, "schema_version": CURRENT_SCHEMA_VERSION,
            **await run_in_threadpool(identity)}
