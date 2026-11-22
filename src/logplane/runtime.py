"""Reusable leased worker runtime for framework-neutral executors."""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Iterable, Optional

import httpx

from logplane.client import BusClient
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    Executor,
    PermanentFailure,
    RetryableFailure,
    ensure_outcome,
    json_size,
    mutable_json,
)
from logplane.limits import MAX_INLINE_DEPENDENCY_BYTES, MAX_INLINE_RESULT_BYTES
from logplane.projection import PMState, PROJECTION_TOPICS, apply_event

DEFAULT_HEARTBEAT_SECONDS = 5.0
DEFAULT_PUBLISH_RETRY_SECONDS = 1.0
DEFAULT_DEPENDENCY_FETCH_ATTEMPTS = 3
RUNTIME_TOPICS = (
    "agent.registered",
    "agent.policy_set",
    "task.assigned",
    "task.assignment_expired",
    "task.failed",
    "task.cancel_requested",
    "task.cancelled",
    "task.deadline_exceeded",
    "task.pause_requested",
    "task.paused",
    "task.resumed",
    "task.superseded",
    "task.created",
    "workflow.pause_requested",
    "workflow.paused",
    "workflow.policy_set",
    "workflow.resumed",
)

_PHASE_ACCEPTED = "accepted"
_PHASE_EXECUTING = "executing"
_PHASE_PUBLISHING_OUTCOME = "publishing_outcome"


class WorkerRuntime:
    """Own bus plumbing while delegating actual work to an Executor.

    Capacity is enforced by a bounded thread pool. Ownership loss cancels
    cancellable adapters and suppresses their eventual lifecycle result. The
    PM reducer remains the final race-safe authority for stale events.
    """

    def __init__(
        self,
        bus: BusClient,
        *,
        name: str,
        executor: Executor,
        capacity: int = 1,
        capabilities: Iterable[str] = (),
        instance_id: Optional[str] = None,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        publish_retry_seconds: float = DEFAULT_PUBLISH_RETRY_SECONDS,
        dependency_fetch_attempts: int = DEFAULT_DEPENDENCY_FETCH_ATTEMPTS,
        unexpected_exceptions_retryable: bool = False,
        log: Callable[[str], None] = print,
    ):
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if heartbeat_seconds <= 0 or publish_retry_seconds <= 0:
            raise ValueError("heartbeat and publish retry intervals must be positive")
        if (
            not isinstance(dependency_fetch_attempts, int)
            or isinstance(dependency_fetch_attempts, bool)
            or dependency_fetch_attempts < 1
        ):
            raise ValueError("dependency_fetch_attempts must be a positive integer")
        normalized_capabilities = tuple(dict.fromkeys(capabilities))
        if not all(isinstance(item, str) and item.strip() for item in normalized_capabilities):
            raise ValueError("capabilities must contain non-empty strings")
        if getattr(bus, "actor", name) != name:
            raise ValueError("bus actor must match worker name")

        self.bus = bus
        self.name = name
        self.executor = executor
        self.capacity = capacity
        self.capabilities = normalized_capabilities
        self.instance_id = instance_id or uuid.uuid4().hex
        self.heartbeat_seconds = heartbeat_seconds
        self.publish_retry_seconds = publish_retry_seconds
        self.dependency_fetch_attempts = dependency_fetch_attempts
        self.unexpected_exceptions_retryable = unexpected_exceptions_retryable
        self.log = log

        self.stop_event = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._lock = threading.Lock()
        self._executor_control_lock = threading.Lock()
        self._owned: set[str] = set()
        self._seen_assignment_ids: set[str] = set()
        self._assignment_phases: dict[str, str] = {}
        self._futures: dict[str, Future] = {}
        self._task_by_assignment: dict[str, int] = {}
        self._correlation_by_assignment: dict[str, str] = {}
        self._deadline_timers: dict[str, threading.Timer] = {}
        self._paused_task_ids: set[int] = set()
        self._paused_correlations: set[str] = set()
        # Admission is derived from the same ordered history as the PM. This
        # cache is private to the serial stream consumer and never authorizes
        # work based on a policy row the shared reducer rejected.
        self._admission_state = PMState()
        self._admission_event_id = 0
        self._accepting = False
        self._closed = False
        self._pool: Optional[ThreadPoolExecutor] = None
        self._heartbeat: Optional[threading.Thread] = None

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._owned)

    def run(self, events: Optional[Iterable[dict]] = None) -> None:
        """Register and consume events until stopped.

        Passing a finite event iterable is useful for embedding and tests; a
        normally exhausted iterable drains accepted work before shutdown. The
        bus must still supply query_all() over the persisted coordination
        history so admission can verify those deliveries.
        """
        registered = self.bus.publish(
            "agent.registered",
            {
                "name": self.name,
                "instance_id": self.instance_id,
                "capacity": self.capacity,
                "capabilities": list(self.capabilities),
            },
            idempotency_key=f"registered:{self.instance_id}",
        )
        self.log(
            f"[{self.name}] registered instance {self.instance_id[:8]}, "
            "waiting for work"
        )
        with self._lock:
            self._accepting = True
        self._pool = ThreadPoolExecutor(
            max_workers=self.capacity,
            thread_name_prefix=f"{self.name}-assignment",
        )
        self._heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"{self.name}-heartbeat",
        )
        self._heartbeat.start()

        completed_normally = False
        try:
            stream = events
            if stream is None:
                stream = self.bus.subscribe(
                    topics=list(RUNTIME_TOPICS),
                    from_id=registered["id"],
                    stop_event=self.stop_event,
                )
            for event in stream:
                if self.stop_event.is_set():
                    break
                self.process_event(event)
                if self.stop_event.is_set():
                    # Do not request another blocking SSE item after an
                    # admission/dependency error has already stopped us.
                    break
            completed_normally = not self.stop_event.is_set()
        finally:
            self.shutdown(drain=completed_normally)

    def process_event(self, event: dict) -> None:
        if not isinstance(event, dict):
            return
        topic = event.get("topic")
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return

        if topic == "agent.registered":
            if (
                payload.get("name") == self.name
                and payload.get("instance_id") != self.instance_id
            ):
                self.log(f"[{self.name}] replacement instance registered; stopping")
                self.stop()
            return

        if topic == "task.assignment_expired":
            if payload.get("worker_instance_id") == self.instance_id:
                self._revoke(payload.get("assignment_id"))
            return

        if topic == "task.failed":
            task_id = payload.get("task_id")
            if isinstance(task_id, int):
                with self._lock:
                    self._paused_task_ids.discard(task_id)
            self._revoke(payload.get("last_assignment_id"))
            return

        if topic in {"task.cancel_requested", "task.pause_requested"}:
            task_id = payload.get("task_id")
            with self._lock:
                if topic == "task.pause_requested" and isinstance(task_id, int):
                    self._paused_task_ids.add(task_id)
                elif topic == "task.cancel_requested" and isinstance(task_id, int):
                    self._paused_task_ids.discard(task_id)
                assignment_ids = tuple(
                    assignment_id
                    for assignment_id, owned_task_id in self._task_by_assignment.items()
                    if owned_task_id == task_id
                )
            for assignment_id in assignment_ids:
                self._revoke(assignment_id)
            return

        if topic == "task.resumed":
            task_id = payload.get("task_id")
            if isinstance(task_id, int):
                with self._lock:
                    self._paused_task_ids.discard(task_id)
            return

        if topic in {"task.cancelled", "task.deadline_exceeded", "task.paused", "task.superseded"}:
            task_id = payload.get("task_id")
            if topic != "task.paused" and isinstance(task_id, int):
                with self._lock:
                    self._paused_task_ids.discard(task_id)
            self._revoke(payload.get("last_assignment_id"))
            return

        if topic == "task.created" and isinstance(payload.get("supersedes_task_id"), int):
            superseded_task_id = payload["supersedes_task_id"]
            with self._lock:
                self._paused_task_ids.discard(superseded_task_id)
                assignment_ids = tuple(
                    assignment_id
                    for assignment_id, owned_task_id in self._task_by_assignment.items()
                    if owned_task_id == superseded_task_id
                )
            for assignment_id in assignment_ids:
                self._revoke(assignment_id)
            return

        if topic in {"workflow.pause_requested", "workflow.paused"}:
            correlation_id = event.get("correlation_id")
            with self._lock:
                if isinstance(correlation_id, str):
                    self._paused_correlations.add(correlation_id)
                assignment_ids = tuple(
                    assignment_id
                    for assignment_id, owned_correlation_id
                    in self._correlation_by_assignment.items()
                    if owned_correlation_id == correlation_id
                )
            for assignment_id in assignment_ids:
                self._revoke(assignment_id)
            return

        if topic == "workflow.resumed":
            correlation_id = event.get("correlation_id")
            if isinstance(correlation_id, str):
                with self._lock:
                    self._paused_correlations.discard(correlation_id)
            return

        if topic != "task.assigned":
            return
        if (
            payload.get("assignee") != self.name
            or payload.get("worker_instance_id") != self.instance_id
        ):
            return
        raw_assignment_id = payload.get("assignment_id")
        task_id = payload.get("task_id")
        correlation_id = event.get("correlation_id")
        if isinstance(raw_assignment_id, str):
            with self._lock:
                if (
                    raw_assignment_id in self._seen_assignment_ids
                    or task_id in self._paused_task_ids
                    or correlation_id in self._paused_correlations
                ):
                    self._seen_assignment_ids.add(raw_assignment_id)
                    return
        try:
            if not self._assignment_is_accepted(event):
                with self._lock:
                    if isinstance(raw_assignment_id, str):
                        self._seen_assignment_ids.add(raw_assignment_id)
                self.log(
                    f"[{self.name}] ignoring rejected assignment: {raw_assignment_id}"
                )
                return
            resolved_event = self._resolve_dependency_refs_with_retry(event)
            if resolved_event is None:
                return
            assignment = AssignmentContext.from_event(resolved_event)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            self.log(
                f"[{self.name}] could not resolve assignment safely: {exc}; stopping"
            )
            self.stop()
            return

        assignment_id = assignment.assignment_id
        deadline_timer: Optional[threading.Timer] = None
        with self._lock:
            if (
                not self._accepting
                or assignment_id in self._seen_assignment_ids
                or self._pool is None
            ):
                return
            # A delivery is consumed once by this runtime instance, even after
            # its execution state has been cleaned up. Replayed SSE events must
            # not execute external effects a second time.
            self._seen_assignment_ids.add(assignment_id)
            if (
                assignment.deadline_at is not None
                and assignment.deadline_at <= time.time()
            ):
                self.log(
                    f"[{self.name}] ignoring already-expired assignment: "
                    f"{assignment_id}"
                )
                return
            self._owned.add(assignment_id)
            self._assignment_phases[assignment_id] = _PHASE_ACCEPTED
            self._task_by_assignment[assignment_id] = assignment.task_id
            if isinstance(correlation_id, str):
                self._correlation_by_assignment[assignment_id] = correlation_id
            if assignment.deadline_at is not None:
                deadline_timer = threading.Timer(
                    assignment.deadline_at - time.time(),
                    self._deadline_elapsed,
                    args=(assignment_id,),
                )
                deadline_timer.daemon = True
                self._deadline_timers[assignment_id] = deadline_timer
            future = self._pool.submit(self._execute_assignment, assignment)
            self._futures[assignment_id] = future
        if deadline_timer is not None:
            deadline_timer.start()

    def _assignment_is_accepted(self, event: dict) -> bool:
        """Replay through this delivery, never past it, before executing it.

        The SSE subscription begins at registration, but the first admission
        must include older policies, tasks, and accounting. Later admissions
        fetch only the unseen coordination suffix. A missing/truncated prefix
        is an error: stopping heartbeats allows the PM to recover ownership.
        Later policy changes do not retroactively revoke accepted assignments;
        cancellation and deadlines retain their separate runtime fences.
        """
        target_id = event.get("id")
        if (
            not isinstance(target_id, int)
            or isinstance(target_id, bool)
            or target_id <= 0
        ):
            raise ValueError("assignment event id must be positive")
        if target_id <= self._admission_event_id:
            return False
        iterator = getattr(self.bus, "iter_events", None)
        if callable(iterator):
            history = iterator(after_id=self._admission_event_id, through_id=target_id,
                               topics=list(PROJECTION_TOPICS))
        else:
            # Compatibility with existing embedded/custom clients. The standard
            # HTTP client uses bounded pages rather than materializing history.
            history = self.bus.query_all(
                after_id=self._admission_event_id, topics=list(PROJECTION_TOPICS)
            )
        for recorded in history:
            if not isinstance(recorded, dict):
                raise ValueError("coordination history contains a non-event")
            recorded_id = recorded.get("id")
            if (
                not isinstance(recorded_id, int)
                or isinstance(recorded_id, bool)
                or recorded_id <= self._admission_event_id
            ):
                raise ValueError("coordination history is not strictly ordered")
            if recorded_id > target_id:
                break
            if recorded_id == target_id and recorded != event:
                raise ValueError("assignment differs from persisted history")
            apply_event(self._admission_state, recorded)
            self._admission_event_id = recorded_id
            if recorded_id == target_id:
                task = self._admission_state.tasks.get(
                    event["payload"].get("task_id")
                )
                return (
                    task is not None
                    and task.status == "assigned"
                    and task.assignment_event_id == target_id
                    and task.assignment_id == event["payload"].get("assignment_id")
                    and task.assignee == self.name
                    and task.worker_instance_id == self.instance_id
                )
        raise ValueError("coordination history does not contain the assignment")

    def _resolve_dependency_refs_with_retry(self, event: dict) -> Optional[dict]:
        for attempt in range(1, self.dependency_fetch_attempts + 1):
            try:
                return self._resolve_dependency_refs(event)
            except httpx.HTTPStatusError as exc:
                if 400 <= exc.response.status_code < 500:
                    raise
                error: httpx.HTTPError = exc
            except httpx.HTTPError as exc:
                error = exc

            if attempt >= self.dependency_fetch_attempts:
                raise error
            self.log(
                f"[{self.name}] dependency lookup failed "
                f"({error.__class__.__name__}); retrying "
                f"({attempt + 1}/{self.dependency_fetch_attempts})"
            )
            if self.stop_event.wait(self.publish_retry_seconds):
                return None
        return None

    def _resolve_dependency_refs(self, event: dict) -> dict:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("assignment payload must be an object")
        refs = payload.get("dependency_refs", [])
        if not isinstance(refs, list):
            raise ValueError("assignment dependency_refs must be a list")
        if not refs:
            return event

        dependencies = []
        for index, ref in enumerate(refs):
            if not isinstance(ref, dict) or set(ref) != {
                "task_id",
                "completion_event_id",
            }:
                raise ValueError(f"dependency_refs[{index}] has an invalid shape")
            task_id = ref.get("task_id")
            completion_event_id = ref.get("completion_event_id")
            if (
                not isinstance(task_id, int)
                or isinstance(task_id, bool)
                or task_id <= 0
                or not isinstance(completion_event_id, int)
                or isinstance(completion_event_id, bool)
                or completion_event_id <= 0
            ):
                raise ValueError(f"dependency_refs[{index}] has invalid ids")
            completion = self.bus.get_event(completion_event_id)
            completion_payload = completion.get("payload")
            if (
                completion.get("id") != completion_event_id
                or completion.get("topic") != "task.completed"
                or completion.get("correlation_id") != event.get("correlation_id")
                or not isinstance(completion_payload, dict)
                or completion_payload.get("task_id") != task_id
            ):
                raise ValueError(
                    f"dependency_refs[{index}] does not identify the expected completion"
                )
            result = completion_payload.get("result", {})
            summary = completion_payload.get("summary", "")
            if not isinstance(result, dict) or not isinstance(summary, str):
                raise ValueError(
                    f"dependency_refs[{index}] completion payload is malformed"
                )
            dependencies.append(
                {
                    "task_id": task_id,
                    "completion_event_id": completion_event_id,
                    "summary": summary,
                    "result": result,
                }
            )

        resolved = dict(event)
        resolved["payload"] = {**payload, "dependencies": dependencies}
        return resolved

    def stop(self) -> None:
        self.stop_event.set()
        self._heartbeat_stop.set()
        with self._lock:
            assignment_ids = tuple(self._owned)
            self._accepting = False
        for assignment_id in assignment_ids:
            self._revoke(assignment_id)

    def shutdown(self, *, drain: bool = False) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._accepting = False
            assignment_ids = tuple(self._owned)
        if not drain:
            self.stop_event.set()
            for assignment_id in assignment_ids:
                self._revoke(assignment_id)
            self._close_executor()
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=not drain)
        if drain:
            self._close_executor()
        self.stop_event.set()
        self._heartbeat_stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=max(1.0, self.heartbeat_seconds + 0.5))

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self.bus.publish(
                    "agent.heartbeat",
                    {"name": self.name, "instance_id": self.instance_id},
                )
            except httpx.HTTPError as exc:
                self.log(
                    f"[{self.name}] heartbeat failed ({exc.__class__.__name__})"
                )

    def _execute_assignment(self, assignment: AssignmentContext) -> None:
        assignment_id = assignment.assignment_id
        lifecycle = {
            "task_id": assignment.task_id,
            "assignment_id": assignment_id,
            "worker_instance_id": self.instance_id,
        }
        try:
            started = self._publish_while_owned(
                assignment_id,
                "task.started",
                lifecycle,
                caused_by=assignment.assignment_event_id,
                idempotency_key=f"started:{assignment_id}",
            )
            if started is None:
                return
            if json_size(assignment.dependencies) > MAX_INLINE_DEPENDENCY_BYTES:
                if not self._transition_phase(
                    assignment,
                    _PHASE_ACCEPTED,
                    _PHASE_PUBLISHING_OUTCOME,
                ):
                    return
                outcome = PermanentFailure(
                    "dependency_input_too_large",
                    "resolved dependency input exceeds the inline execution limit",
                )
            else:
                if not self._transition_phase(
                    assignment,
                    _PHASE_ACCEPTED,
                    _PHASE_EXECUTING,
                ):
                    return
                self.log(
                    f"[{self.name}] executing task {assignment.task_id} "
                    f"attempt {assignment.attempt}: {assignment.goal}"
                )
                try:
                    outcome = ensure_outcome(self.executor.execute(assignment))
                except Exception as exc:
                    reason = f"executor raised {exc.__class__.__name__}: {exc}"
                    if self.unexpected_exceptions_retryable:
                        outcome = RetryableFailure("executor_exception", reason)
                    else:
                        outcome = PermanentFailure("executor_exception", reason)
                if not self._transition_phase(
                    assignment,
                    _PHASE_EXECUTING,
                    _PHASE_PUBLISHING_OUTCOME,
                    check_deadline=False,
                ):
                    return

            if isinstance(outcome, Completed):
                if json_size(outcome.result) > MAX_INLINE_RESULT_BYTES:
                    outcome = PermanentFailure(
                        "result_too_large",
                        "executor result exceeds the inline coordination limit",
                    )
                else:
                    payload = {**lifecycle, "summary": outcome.summary}
                    if outcome.result:
                        payload["result"] = mutable_json(outcome.result)
                    self._publish_while_owned(
                        assignment_id,
                        "task.completed",
                        payload,
                        caused_by=assignment.assignment_event_id,
                        idempotency_key=f"completed:{assignment_id}",
                    )
                    return

            if isinstance(outcome, Blocked):
                self._publish_while_owned(
                    assignment_id,
                    "task.blocked",
                    {**lifecycle, "reason": outcome.reason},
                    caused_by=assignment.assignment_event_id,
                    idempotency_key=f"blocked:{assignment_id}",
                )
                return

            retryable = isinstance(outcome, RetryableFailure)
            self._publish_while_owned(
                assignment_id,
                "task.attempt_failed",
                {
                    **lifecycle,
                    "failure_code": outcome.code,
                    "reason": outcome.reason,
                    "retryable": retryable,
                    **(
                        {"retry_after_seconds": outcome.retry_after_seconds}
                        if retryable and outcome.retry_after_seconds is not None
                        else {}
                    ),
                },
                caused_by=assignment.assignment_event_id,
                idempotency_key=f"attempt-failed:{assignment_id}",
            )
        finally:
            deadline_timer = None
            with self._lock:
                self._owned.discard(assignment_id)
                self._assignment_phases.pop(assignment_id, None)
                self._futures.pop(assignment_id, None)
                self._task_by_assignment.pop(assignment_id, None)
                self._correlation_by_assignment.pop(assignment_id, None)
                deadline_timer = self._deadline_timers.pop(assignment_id, None)
            if deadline_timer is not None:
                deadline_timer.cancel()

    def _publish_while_owned(
        self,
        assignment_id: str,
        topic: str,
        payload: dict,
        *,
        caused_by: int,
        idempotency_key: str,
    ) -> Optional[dict]:
        while self._owns(assignment_id) and not self.stop_event.is_set():
            try:
                return self.bus.publish(
                    topic,
                    payload,
                    caused_by=caused_by,
                    idempotency_key=idempotency_key,
                )
            except httpx.HTTPStatusError as exc:
                if 400 <= exc.response.status_code < 500:
                    self.log(
                        f"[{self.name}] bus rejected {topic} with "
                        f"HTTP {exc.response.status_code}; stopping this instance"
                    )
                    self.stop_event.set()
                    return None
                self.log(
                    f"[{self.name}] publish failed ({exc.__class__.__name__}); "
                    f"retrying {topic}"
                )
                if self.stop_event.wait(self.publish_retry_seconds):
                    break
            except httpx.HTTPError as exc:
                self.log(
                    f"[{self.name}] publish failed ({exc.__class__.__name__}); "
                    f"retrying {topic}"
                )
                if self.stop_event.wait(self.publish_retry_seconds):
                    break
        return None

    def _owns(self, assignment_id: str) -> bool:
        with self._lock:
            return assignment_id in self._owned

    def _transition_phase(
        self,
        assignment: AssignmentContext,
        expected: str,
        target: str,
        *,
        check_deadline: bool = True,
    ) -> bool:
        assignment_id = assignment.assignment_id
        expired = False
        with self._lock:
            if (
                assignment_id not in self._owned
                or self._assignment_phases.get(assignment_id) != expected
            ):
                return False
            if (
                check_deadline
                and assignment.deadline_at is not None
                and assignment.deadline_at <= time.time()
            ):
                expired = True
            else:
                self._assignment_phases[assignment_id] = target
        if expired:
            self._deadline_elapsed(assignment_id)
            return False
        return True

    def _deadline_elapsed(self, assignment_id: str) -> None:
        self.log(f"[{self.name}] assignment deadline reached: {assignment_id}")
        self._revoke(assignment_id)

    def _revoke(self, assignment_id: object) -> None:
        if not isinstance(assignment_id, str):
            return
        with self._lock:
            if assignment_id not in self._owned:
                return
            self._owned.discard(assignment_id)
            self._task_by_assignment.pop(assignment_id, None)
            self._correlation_by_assignment.pop(assignment_id, None)
            phase = self._assignment_phases.pop(assignment_id, None)
            future = self._futures.get(assignment_id)
            deadline_timer = self._deadline_timers.pop(assignment_id, None)
        if deadline_timer is not None:
            deadline_timer.cancel()
        cancelled_before_start = False
        if future is not None:
            cancelled_before_start = future.cancel()
            if cancelled_before_start:
                with self._lock:
                    self._futures.pop(assignment_id, None)
        if not cancelled_before_start and phase == _PHASE_EXECUTING:
            cancel = getattr(self.executor, "cancel", None)
            if callable(cancel):
                try:
                    with self._executor_control_lock:
                        cancel(assignment_id)
                except Exception as exc:
                    self.log(
                        f"[{self.name}] executor cancellation failed for "
                        f"{assignment_id}: {exc.__class__.__name__}: {exc}"
                    )

    def _close_executor(self) -> None:
        close = getattr(self.executor, "close", None)
        if callable(close):
            with self._executor_control_lock:
                close()
