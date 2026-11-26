"""Framework-neutral executor contract and reference adapters."""

from __future__ import annotations

import hashlib
import json
import math
import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence, Union

from logplane.executor_protocol import (
    LEGACY_PROTOCOL_VERSION,
    assignment_message,
    parse_outcome_message,
    validate_protocol_version,
)
from logplane.limits import MAX_RETRY_AFTER_SECONDS, MAX_TASK_DEPENDENCIES

DEFAULT_MAX_PROTOCOL_BYTES = 64 * 1024


def _positive_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def mutable_json(value: Any) -> Any:
    """Return a JSON-serializable mutable copy of an immutable JSON value."""
    return _thaw(value)


def _immutable_json_object(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    try:
        normalized = json.loads(
            json.dumps(dict(value), allow_nan=False, separators=(",", ":"))
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must contain JSON-compatible values") from exc
    return _freeze(normalized)


def _immutable_decisions(
    value: object,
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("decisions must be a JSON array")
    try:
        normalized = json.loads(
            json.dumps(list(value), allow_nan=False, separators=(",", ":"))
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("decisions must contain JSON-compatible values") from exc
    for index, decision in enumerate(normalized):
        if not isinstance(decision, dict):
            raise ValueError(f"decisions[{index}] must be an object")
        if set(decision) != {
            "event_id",
            "actor",
            "assignment_id",
            "decision_id",
            "decision",
        }:
            raise ValueError(f"decisions[{index}] has an invalid shape")
        _positive_int(decision["event_id"], f"decisions[{index}].event_id")
        for field_name in ("actor", "assignment_id", "decision_id"):
            _nonempty_string(
                decision[field_name],
                f"decisions[{index}].{field_name}",
            )
    return _freeze(normalized)


def _immutable_dependencies(
    value: object,
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("dependencies must be a JSON array")
    if len(value) > MAX_TASK_DEPENDENCIES:
        raise ValueError(
            f"dependencies must contain at most {MAX_TASK_DEPENDENCIES} entries"
        )
    try:
        normalized = json.loads(
            json.dumps(list(value), allow_nan=False, separators=(",", ":"))
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("dependencies must contain JSON-compatible values") from exc
    seen: set[int] = set()
    for index, dependency in enumerate(normalized):
        if not isinstance(dependency, dict) or set(dependency) != {
            "task_id",
            "completion_event_id",
            "summary",
            "result",
        }:
            raise ValueError(f"dependencies[{index}] has an invalid shape")
        task_id = _positive_int(
            dependency["task_id"], f"dependencies[{index}].task_id"
        )
        _positive_int(
            dependency["completion_event_id"],
            f"dependencies[{index}].completion_event_id",
        )
        if not isinstance(dependency["summary"], str):
            raise ValueError(f"dependencies[{index}].summary must be a string")
        if not isinstance(dependency["result"], dict):
            raise ValueError(f"dependencies[{index}].result must be a JSON object")
        if task_id in seen:
            raise ValueError("dependencies must not contain duplicate task ids")
        seen.add(task_id)
    return _freeze(normalized)


def json_size(value: object) -> int:
    return len(
        json.dumps(
            _thaw(value),
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


@dataclass(frozen=True)
class AssignmentContext:
    """Immutable input presented to every executor implementation."""

    correlation_id: Optional[str]
    task_id: int
    assignment_id: str
    assignment_event_id: int
    attempt: int
    goal: str
    context: Mapping[str, Any] = field(default_factory=dict)
    required_capabilities: tuple[str, ...] = ()
    max_retries: Optional[int] = None
    retryable_failures: int = 0
    assignee: str = ""
    worker_instance_id: str = ""
    ownership_mode: str = "controlled"
    # Owner value stored in event history; see adoption.ExecutionOwner.
    ownership_owner: str = "agent-bus"
    external_origin: Optional[Mapping[str, Any]] = None
    decisions: tuple[Mapping[str, Any], ...] = ()
    dependencies: tuple[Mapping[str, Any], ...] = ()
    deadline_at: Optional[float] = None
    workflow_policy_event_id: Optional[int] = None
    fairness_policy: Optional[str] = None
    previous_assignment_event_id: Optional[int] = None
    agent_policy_event_id: Optional[int] = None
    budget_policy_event_id: Optional[int] = None
    reserved_tokens: int = 0
    reserved_cost_usd: float = 0.0

    def __post_init__(self) -> None:
        _positive_int(self.task_id, "task_id")
        _positive_int(self.assignment_event_id, "assignment_event_id")
        _positive_int(self.attempt, "attempt")
        _nonempty_string(self.assignment_id, "assignment_id")
        _nonempty_string(self.goal, "goal")
        _nonempty_string(self.assignee, "assignee")
        _nonempty_string(self.worker_instance_id, "worker_instance_id")
        if self.correlation_id is not None:
            _nonempty_string(self.correlation_id, "correlation_id")
        if self.max_retries is not None:
            _nonnegative_int(self.max_retries, "max_retries")
        if self.workflow_policy_event_id is not None:
            _positive_int(
                self.workflow_policy_event_id,
                "workflow_policy_event_id",
            )
        if self.fairness_policy is not None:
            if self.fairness_policy != "workflow_round_robin_v1":
                raise ValueError(
                    "fairness_policy must be workflow_round_robin_v1 or None"
                )
            if self.previous_assignment_event_id is not None:
                _positive_int(
                    self.previous_assignment_event_id,
                    "previous_assignment_event_id",
                )
        elif self.previous_assignment_event_id is not None:
            raise ValueError(
                "previous_assignment_event_id requires fairness_policy"
            )
        if self.agent_policy_event_id is not None:
            _positive_int(self.agent_policy_event_id, "agent_policy_event_id")
        _nonnegative_int(self.reserved_tokens, "reserved_tokens")
        if (
            not isinstance(self.reserved_cost_usd, (int, float))
            or isinstance(self.reserved_cost_usd, bool)
            or not math.isfinite(self.reserved_cost_usd)
            or self.reserved_cost_usd < 0
        ):
            raise ValueError("reserved_cost_usd must be a non-negative number")
        if self.budget_policy_event_id is not None:
            _positive_int(self.budget_policy_event_id, "budget_policy_event_id")
            if self.budget_policy_event_id != self.workflow_policy_event_id:
                raise ValueError(
                    "budget_policy_event_id must match workflow_policy_event_id"
                )
        elif self.reserved_tokens or self.reserved_cost_usd:
            raise ValueError("non-zero reservation requires budget_policy_event_id")
        _nonnegative_int(self.retryable_failures, "retryable_failures")
        if self.deadline_at is not None and (
            not isinstance(self.deadline_at, (int, float))
            or isinstance(self.deadline_at, bool)
            or not math.isfinite(self.deadline_at)
            or self.deadline_at <= 0
        ):
            raise ValueError(
                "deadline_at must be a positive finite timestamp or None"
            )
        if not isinstance(self.required_capabilities, tuple) or not all(
            isinstance(item, str) and item.strip()
            for item in self.required_capabilities
        ):
            raise ValueError("required_capabilities must be a tuple of strings")
        if self.ownership_mode not in {"controlled", "canary"}:
            raise ValueError("assigned work must use controlled or canary mode")
        if self.ownership_owner != "agent-bus":
            raise ValueError("assigned work must be owned by logplane")
        object.__setattr__(
            self,
            "context",
            _immutable_json_object(self.context, "context"),
        )
        object.__setattr__(self, "decisions", _immutable_decisions(self.decisions))
        object.__setattr__(
            self,
            "dependencies",
            _immutable_dependencies(self.dependencies),
        )
        if self.external_origin is not None:
            object.__setattr__(
                self,
                "external_origin",
                _immutable_json_object(self.external_origin, "external_origin"),
            )

    @classmethod
    def from_event(cls, event: Mapping[str, Any]) -> "AssignmentContext":
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("assignment event payload must be an object")
        retry_policy = payload.get("retry_policy", {})
        if not isinstance(retry_policy, Mapping):
            raise ValueError("assignment retry_policy must be an object")
        ownership = payload.get(
            "ownership",
            {"mode": "controlled", "owner": "agent-bus"},
        )
        if not isinstance(ownership, Mapping):
            raise ValueError("assignment ownership must be an object")
        raw_capabilities = payload.get("required_capabilities", [])
        if not isinstance(raw_capabilities, list):
            raise ValueError("required_capabilities must be a list")
        dependency_refs = payload.get("dependency_refs", [])
        dependencies = payload.get("dependencies")
        if dependency_refs and dependencies is None:
            raise ValueError(
                "assignment dependency_refs must be resolved before parsing"
            )
        if dependencies is None:
            dependencies = []
        fairness = payload.get("fairness")
        if fairness is None:
            fairness = {}
        elif not isinstance(fairness, Mapping):
            raise ValueError("assignment fairness must be an object")
        elif set(fairness) != {"policy", "previous_assignment_event_id"}:
            raise ValueError(
                "assignment fairness must contain policy and previous_assignment_event_id"
            )
        reservation = payload.get("budget_reservation")
        if reservation is None:
            reservation = {}
        elif not isinstance(reservation, Mapping) or set(reservation) != {
            "policy_event_id",
            "tokens",
            "cost_usd",
        }:
            raise ValueError("assignment budget_reservation has an invalid shape")
        assignment = cls(
            correlation_id=event.get("correlation_id"),
            task_id=payload.get("task_id"),
            assignment_id=payload.get("assignment_id"),
            assignment_event_id=event.get("id"),
            attempt=payload.get("attempt"),
            goal=payload.get("goal") or payload.get("title"),
            context=payload.get("context", {}),
            decisions=payload.get("decisions", []),
            required_capabilities=tuple(raw_capabilities),
            max_retries=retry_policy.get("max_retries"),
            retryable_failures=payload.get("retryable_failures", 0),
            assignee=payload.get("assignee"),
            worker_instance_id=payload.get("worker_instance_id"),
            ownership_mode=ownership.get("mode"),
            ownership_owner=ownership.get("owner"),
            external_origin=payload.get("external_origin"),
            dependencies=dependencies,
            deadline_at=payload.get("deadline_at"),
            workflow_policy_event_id=payload.get("workflow_policy_event_id"),
            fairness_policy=fairness.get("policy"),
            previous_assignment_event_id=fairness.get(
                "previous_assignment_event_id"
            ),
            agent_policy_event_id=payload.get("agent_policy_event_id"),
            budget_policy_event_id=reservation.get("policy_event_id"),
            reserved_tokens=reservation.get("tokens", 0),
            reserved_cost_usd=reservation.get("cost_usd", 0.0),
        )
        if not isinstance(dependency_refs, list):
            raise ValueError("assignment dependency_refs must be a list")
        expected_refs = [
            {
                "task_id": dependency["task_id"],
                "completion_event_id": dependency["completion_event_id"],
            }
            for dependency in assignment.dependencies
        ]
        if dependency_refs != expected_refs:
            raise ValueError(
                "resolved dependencies must match assignment dependency_refs"
            )
        return assignment

    @property
    def effect_scope(self) -> str:
        """Stable identity for external effects across delivery attempts."""
        # The "agent-bus.*" salts predate the logplane name. Changing them
        # would change every effect key, so a retried task could repeat an
        # external effect that already happened.
        material = json.dumps(
            ["agent-bus.effect-scope.v1", self.correlation_id, self.task_id],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"effect-scope:{hashlib.sha256(material).hexdigest()}"

    def effect_id(self, operation_name: str) -> str:
        """Return a deterministic idempotency key for one logical operation."""
        operation = _nonempty_string(operation_name, "operation_name").strip()
        material = json.dumps(
            ["agent-bus.effect.v1", self.effect_scope, operation],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"effect:{hashlib.sha256(material).hexdigest()}"

    def to_dict(self) -> dict:
        result = {
            "correlation_id": self.correlation_id,
            "task_id": self.task_id,
            "assignment_id": self.assignment_id,
            "assignment_event_id": self.assignment_event_id,
            "effect_scope": self.effect_scope,
            "attempt": self.attempt,
            "goal": self.goal,
            "context": _thaw(self.context),
            "decisions": _thaw(self.decisions),
            "dependencies": _thaw(self.dependencies),
            "required_capabilities": list(self.required_capabilities),
            "retry_policy": {"max_retries": self.max_retries},
            "retryable_failures": self.retryable_failures,
            "deadline_at": self.deadline_at,
            "workflow_policy_event_id": self.workflow_policy_event_id,
            "agent_policy_event_id": self.agent_policy_event_id,
            "assignee": self.assignee,
            "worker_instance_id": self.worker_instance_id,
            "ownership": {
                "mode": self.ownership_mode,
                "owner": self.ownership_owner,
            },
        }
        if self.external_origin is not None:
            result["external_origin"] = _thaw(self.external_origin)
        if self.fairness_policy is not None:
            result["fairness"] = {
                "policy": self.fairness_policy,
                "previous_assignment_event_id": self.previous_assignment_event_id,
            }
        if self.budget_policy_event_id is not None:
            result["budget_reservation"] = {
                "policy_event_id": self.budget_policy_event_id,
                "tokens": self.reserved_tokens,
                "cost_usd": self.reserved_cost_usd,
            }
        return result


@dataclass(frozen=True)
class Completed:
    summary: str
    result: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _nonempty_string(self.summary, "summary")
        object.__setattr__(
            self,
            "result",
            _immutable_json_object(self.result, "result"),
        )


@dataclass(frozen=True)
class Blocked:
    reason: str

    def __post_init__(self) -> None:
        _nonempty_string(self.reason, "reason")


@dataclass(frozen=True)
class RetryableFailure:
    """A failure worth another attempt.

    ``retry_after_seconds`` asks Logplane not to start the next attempt before
    that long has passed, for a failure with a known recovery time (a
    provider's usage limit, a ``Retry-After``). The retry still counts against
    the task's retry budget.
    """

    code: str
    reason: str
    retry_after_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        _nonempty_string(self.code, "code")
        _nonempty_string(self.reason, "reason")
        delay = self.retry_after_seconds
        if delay is not None:
            if (
                not isinstance(delay, (int, float))
                or isinstance(delay, bool)
                or not math.isfinite(delay)
                or not 0 < delay <= MAX_RETRY_AFTER_SECONDS
            ):
                raise ValueError(
                    "retry_after_seconds must be a positive number of at most "
                    f"{MAX_RETRY_AFTER_SECONDS} seconds"
                )


@dataclass(frozen=True)
class PermanentFailure:
    code: str
    reason: str

    def __post_init__(self) -> None:
        _nonempty_string(self.code, "code")
        _nonempty_string(self.reason, "reason")


Outcome = Union[Completed, Blocked, RetryableFailure, PermanentFailure]
OUTCOME_TYPES = (Completed, Blocked, RetryableFailure, PermanentFailure)


class Executor(Protocol):
    def execute(self, assignment: AssignmentContext) -> Outcome:
        """Perform one assignment and return one explicit lifecycle outcome."""


def ensure_outcome(value: object) -> Outcome:
    if not isinstance(value, OUTCOME_TYPES):
        raise TypeError(
            "executor must return Completed, Blocked, RetryableFailure, "
            "or PermanentFailure"
        )
    return value


def outcome_from_dict(value: object) -> Outcome:
    if not isinstance(value, dict):
        raise ValueError("executor output must be a JSON object")
    status = value.get("status")
    if status == "completed":
        return Completed(
            summary=value.get("summary"),
            result=value.get("result", {}),
        )
    if status == "blocked":
        return Blocked(reason=value.get("reason"))
    if status == "retryable_failure":
        return RetryableFailure(
            code=value.get("code"),
            reason=value.get("reason"),
            retry_after_seconds=value.get("retry_after_seconds"),
        )
    if status == "permanent_failure":
        return PermanentFailure(
            code=value.get("code"),
            reason=value.get("reason"),
        )
    raise ValueError(f"unknown executor outcome status: {status!r}")


class InProcessExecutor:
    """Adapt a Python callable or an object exposing ``run(assignment)``."""

    def __init__(self, target: object):
        run_method = getattr(target, "run", None)
        if callable(run_method):
            self._call: Callable[[AssignmentContext], object] = run_method
        elif callable(target):
            self._call = target
        else:
            raise TypeError("target must be callable or expose a callable run method")
        cancel = getattr(target, "cancel", None)
        self._cancel = cancel if callable(cancel) else None
        close = getattr(target, "close", None)
        self._close = close if callable(close) else None

    def execute(self, assignment: AssignmentContext) -> Outcome:
        return ensure_outcome(self._call(assignment))

    def cancel(self, assignment_id: str) -> None:
        if self._cancel is not None:
            self._cancel(assignment_id)

    def close(self) -> None:
        if self._close is not None:
            self._close()


SUBPROCESS_STOP_GRACE_SECONDS = 1.0


def _signal_process_group(process: subprocess.Popen, sig: int) -> bool:
    """Signal the command's own process group; False when none could be.

    Never raises. ProcessLookupError means the group is gone. PermissionError
    is what macOS reports for a group with only zombies left in it, and
    anywhere else it means we may not signal it; either way nothing more can
    be done here.
    """
    try:
        os.killpg(process.pid, sig)
        return True
    except (ProcessLookupError, PermissionError):
        return False


class SubprocessExecutor:
    """Run a CLI executor using bounded JSON input and output contracts.

    Exit code 0 must produce one JSON outcome on stdout. Exit code 75 is a
    retryable process failure; other non-zero exits are permanent failures.
    Shell execution is deliberately disabled.

    On POSIX the command runs in its own session, and every way an attempt
    ends (exit, timeout, cancellation, oversized output, close) stops the
    whole process group, so processes the command started do not outlive the
    attempt. Elsewhere only the command itself is stopped.
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        timeout: Optional[float] = None,
        max_protocol_bytes: int = DEFAULT_MAX_PROTOCOL_BYTES,
        protocol_version: int = LEGACY_PROTOCOL_VERSION,
        working_directory: Optional[str | Path] = None,
    ):
        if (
            not isinstance(command, Sequence)
            or isinstance(command, (str, bytes))
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            raise ValueError("command must be a non-empty sequence of strings")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        _positive_int(max_protocol_bytes, "max_protocol_bytes")
        validate_protocol_version(protocol_version)
        normalized_directory = None
        if working_directory is not None:
            normalized_directory = Path(working_directory).expanduser().resolve()
            if not normalized_directory.is_dir():
                raise ValueError("working_directory must be an existing directory")
        self.command = tuple(command)
        self.timeout = timeout
        self.max_protocol_bytes = max_protocol_bytes
        self.protocol_version = protocol_version
        self.working_directory = normalized_directory
        self._lock = threading.Lock()
        self._processes: dict[str, subprocess.Popen] = {}
        self._active_calls: set[str] = set()
        self._cancelled: set[str] = set()
        self._closed = False

    def execute(self, assignment: AssignmentContext) -> Outcome:
        assignment_id = assignment.assignment_id
        with self._lock:
            if self._closed:
                return PermanentFailure(
                    "executor_closed",
                    "subprocess executor is closed",
                )
            if assignment_id in self._active_calls:
                return PermanentFailure(
                    "duplicate_assignment",
                    "subprocess assignment is already executing",
                )
            self._active_calls.add(assignment_id)
        try:
            return self._execute_active(assignment)
        finally:
            with self._lock:
                self._active_calls.discard(assignment_id)
                self._cancelled.discard(assignment_id)

    def _execute_active(self, assignment: AssignmentContext) -> Outcome:
        input_bytes = json.dumps(
            assignment_message(assignment.to_dict(), self.protocol_version),
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(input_bytes) > self.max_protocol_bytes:
            return PermanentFailure(
                "assignment_too_large",
                "serialized assignment exceeds the subprocess protocol limit",
            )

        with self._lock:
            if self._closed:
                return PermanentFailure(
                    "executor_closed",
                    "subprocess executor is closed",
                )

        try:
            process = subprocess.Popen(
                self.command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_directory,
                shell=False,
                # Its own session, so its process group contains exactly the
                # command and whatever it starts: see _terminate.
                start_new_session=os.name == "posix",
            )
        except OSError as exc:
            return PermanentFailure(
                "subprocess_start_failed",
                f"could not start subprocess executor: {exc}",
            )

        with self._lock:
            if self._closed or assignment.assignment_id in self._cancelled:
                self._terminate(process)
                self._cancelled.discard(assignment.assignment_id)
                return RetryableFailure(
                    "subprocess_cancelled",
                    "subprocess execution lost assignment ownership",
                )
            self._processes[assignment.assignment_id] = process

        stdout = bytearray()
        stderr = bytearray()
        overflow = threading.Event()
        readers = (
            threading.Thread(
                target=self._read_bounded_pipe,
                args=(process.stdout, stdout, overflow, process),
                daemon=True,
            ),
            threading.Thread(
                target=self._read_bounded_pipe,
                args=(process.stderr, stderr, overflow, process),
                daemon=True,
            ),
        )
        for reader in readers:
            reader.start()
        timed_out = False
        io_error: Optional[OSError] = None
        try:
            try:
                if process.stdin is not None:
                    process.stdin.write(input_bytes)
                    process.stdin.close()
            except BrokenPipeError:
                pass
            try:
                process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                self._terminate(process)
        except OSError as exc:
            io_error = exc
            self._terminate(process)
        finally:
            # Also after a normal exit: a child the command left running would
            # keep working unsupervised and hold the output pipes open.
            self._terminate(process)
            for reader in readers:
                reader.join(timeout=1)
            with self._lock:
                self._processes.pop(assignment.assignment_id, None)

        with self._lock:
            cancelled = assignment.assignment_id in self._cancelled
            self._cancelled.discard(assignment.assignment_id)
        if cancelled:
            return RetryableFailure(
                "subprocess_cancelled",
                "subprocess execution lost assignment ownership",
            )
        if io_error is not None:
            return RetryableFailure(
                "subprocess_io_failed",
                f"subprocess I/O failed after start: {io_error}",
            )
        if timed_out:
            return RetryableFailure(
                "subprocess_timeout",
                "subprocess executor exceeded its timeout",
            )
        if overflow.is_set():
            return PermanentFailure(
                "protocol_output_too_large",
                "subprocess output exceeds the protocol limit",
            )

        stderr_text = bytes(stderr).decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            reason = stderr_text or f"subprocess exited with {process.returncode}"
            if process.returncode == 75:
                return RetryableFailure("subprocess_exit", reason)
            return PermanentFailure("subprocess_exit", reason)
        try:
            decoded = json.loads(bytes(stdout).decode("utf-8"))
            return outcome_from_dict(
                parse_outcome_message(decoded, self.protocol_version)
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return PermanentFailure(
                "invalid_executor_output",
                f"subprocess returned invalid JSON outcome: {exc}",
            )

    def cancel(self, assignment_id: str) -> None:
        with self._lock:
            if assignment_id not in self._active_calls:
                return
            process = self._processes.get(assignment_id)
            self._cancelled.add(assignment_id)
        if process is not None:
            self._terminate(process)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            processes = list(self._processes.items())
            self._cancelled.update(self._active_calls)
        for _, process in processes:
            self._terminate(process)

    def _read_bounded_pipe(
        self,
        pipe,
        output: bytearray,
        overflow: threading.Event,
        process: subprocess.Popen,
    ) -> None:
        if pipe is None:
            return
        try:
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    return
                remaining = self.max_protocol_bytes + 1 - len(output)
                if remaining > 0:
                    output.extend(chunk[:remaining])
                if len(output) > self.max_protocol_bytes or len(chunk) > remaining:
                    overflow.set()
                    self._terminate(process)
                    return
        finally:
            pipe.close()

    @staticmethod
    def _terminate(process: subprocess.Popen) -> None:
        """Stop the command and, on POSIX, everything in its process group.

        Safe to call more than once and after the command has exited; that is
        how children it left behind are stopped.
        """
        if os.name != "posix":
            if process.poll() is not None:
                return
            process.terminate()
            try:
                process.wait(timeout=SUBPROCESS_STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=SUBPROCESS_STOP_GRACE_SECONDS)
            return

        if _signal_process_group(process, signal.SIGTERM):
            deadline = time.monotonic() + SUBPROCESS_STOP_GRACE_SECONDS
            try:
                process.wait(timeout=SUBPROCESS_STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                pass
            # The command exiting is not the group exiting. Give the rest of
            # the group what is left of the grace period, then insist.
            while time.monotonic() < deadline and _signal_process_group(process, 0):
                time.sleep(0.02)
            _signal_process_group(process, signal.SIGKILL)
        try:
            process.wait(timeout=SUBPROCESS_STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pass
