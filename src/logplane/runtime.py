"""Reusable leased worker runtime for framework-neutral executors."""

from __future__ import annotations

import math
import os
import signal
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import wait as wait_for_futures
from typing import Callable, Iterable, Optional, Union

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
DISCONNECT_TIMEOUT_ENV = "LOGPLANE_WORKER_DISCONNECT_TIMEOUT_SECONDS"
# Exit statuses for worker entry points (see WorkerRuntime.exit_code). 75 is
# sysexits EX_TEMPFAIL: the worker stopped because it could not reach the bus
# and is safe to start again. 1 is any other failure that made it stop itself.
DISCONNECT_EXIT_CODE = 75
ERROR_EXIT_CODE = 1
# Read timeout on the event stream when a disconnect timeout is set. The bus
# sends every stream a keepalive after 15s of sending it nothing, so this much
# silence means the connection is gone.
STREAM_READ_TIMEOUT_SECONDS = 30.0
# How long an executor has to return after being told to cancel, by an
# attempt timeout or by the worker stopping. Past this the worker stops
# waiting for it: after an attempt timeout it reports the timeout itself and
# retires, because the slot that executor still occupies cannot be reclaimed;
# when stopping it lets go of its lease and announces the stop regardless.
ATTEMPT_CANCEL_GRACE_SECONDS = 10.0
# How long that worker keeps trying to deliver the timeout report before it
# stops anyway and leaves the task to its lease.
ATTEMPT_REPORT_SECONDS = 10.0
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


def _refusal_status(exc: Optional[BaseException]) -> Optional[int]:
    """The 4xx status when the bus was reached and refused the request."""
    if isinstance(exc, httpx.HTTPStatusError) and 400 <= exc.response.status_code < 500:
        return exc.response.status_code
    return None


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
        disconnect_timeout_seconds: Optional[float] = None,
        attempt_timeout_seconds: Optional[float] = None,
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
        timeout_setting = "disconnect_timeout_seconds"
        if disconnect_timeout_seconds is None:
            configured = os.environ.get(DISCONNECT_TIMEOUT_ENV)
            if configured is not None:
                timeout_setting = DISCONNECT_TIMEOUT_ENV
                try:
                    disconnect_timeout_seconds = float(configured)
                except ValueError as exc:
                    raise ValueError(
                        f"{timeout_setting} must be a positive finite duration in seconds"
                    ) from exc
        if disconnect_timeout_seconds is not None:
            if (
                isinstance(disconnect_timeout_seconds, bool)
                or not isinstance(disconnect_timeout_seconds, (int, float))
                or not 0 < disconnect_timeout_seconds <= threading.TIMEOUT_MAX
                or not math.isfinite(disconnect_timeout_seconds)
            ):
                raise ValueError(
                    f"{timeout_setting} must be a positive finite duration in seconds"
                )
            # The timer starts at the first failed write and only a later
            # confirmed write disarms it, so anything shorter than two
            # heartbeat intervals stops the worker before a retry is even
            # sent. This is a floor, not a guarantee: a retry that succeeds
            # but is answered after the timer fires is still too late.
            if disconnect_timeout_seconds < 2 * heartbeat_seconds:
                raise ValueError(
                    f"{timeout_setting} must be at least twice the heartbeat interval "
                    f"({heartbeat_seconds:g}s)"
                )
        self.disconnect_timeout_seconds = disconnect_timeout_seconds
        if attempt_timeout_seconds is not None and (
            isinstance(attempt_timeout_seconds, bool)
            or not isinstance(attempt_timeout_seconds, (int, float))
            or not 0 < attempt_timeout_seconds <= threading.TIMEOUT_MAX
            or not math.isfinite(attempt_timeout_seconds)
        ):
            raise ValueError(
                "attempt_timeout_seconds must be a positive finite duration in seconds"
            )
        # Wall-clock limit on one execution of the executor, for executors
        # that have no limit of their own. None leaves attempts unbounded.
        self.attempt_timeout_seconds = attempt_timeout_seconds
        self._disconnect_timer: Optional[threading.Timer] = None
        # True once this worker has stopped because it could not reach the
        # bus: the disconnect timeout ran out, or a read it needs before it may
        # run an assignment failed. run() returns as it does for any stop;
        # callers read this, or exit_code, to tell the cases apart.
        self.stopped_by_disconnect = False
        self._failure_exit_code = 0
        self.unexpected_exceptions_retryable = unexpected_exceptions_retryable
        self.log = log

        self.stop_event = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._lock = threading.Lock()
        self._executor_control_lock = threading.Lock()
        self._executor_closed = False
        self._release_thread: Optional[threading.Thread] = None
        self._owned: set[str] = set()
        self._seen_assignment_ids: set[str] = set()
        self._assignment_phases: dict[str, str] = {}
        self._futures: dict[str, Future] = {}
        self._task_by_assignment: dict[str, int] = {}
        self._correlation_by_assignment: dict[str, str] = {}
        self._deadline_timers: dict[str, threading.Timer] = {}
        self._attempt_timers: dict[str, threading.Timer] = {}
        self._timed_out: set[str] = set()
        self._abandoned: set[str] = set()
        self._registered = False
        self._stop_announced = False
        self._stopping_announced = False
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

    @property
    def exit_code(self) -> int:
        """Process exit status for a worker whose run() has returned.

        0 when it was asked to stop (a signal, stop(), a replacement instance
        registering, or a finished event iterable). DISCONNECT_EXIT_CODE when
        it stopped because the bus could not be reached. ERROR_EXIT_CODE when
        it stopped itself over anything else, such as the bus rejecting one of
        its writes. A supervisor can therefore restart on failure only.
        """
        if self.stopped_by_disconnect:
            return DISCONNECT_EXIT_CODE
        return self._failure_exit_code

    def _stopping_on_failure(self, exc: Optional[BaseException] = None) -> None:
        """Record why the worker is about to stop itself, for exit_code."""
        if isinstance(exc, httpx.HTTPError) and _refusal_status(exc) is None:
            self.stopped_by_disconnect = True
        else:
            self._failure_exit_code = ERROR_EXIT_CODE

    def _announce_stopping(self) -> None:
        """Tell the coordinator to send this process nothing more. Best effort, once.

        Said as soon as the worker knows it is going while work is still
        running. The lease on that work stays alive, so nothing is handed to
        another worker while its executors may still be busy; but the
        coordinator stops choosing this worker for new assignments, which it
        would no longer pick up.
        """
        with self._lock:
            if (
                self._stopping_announced
                or self._stop_announced
                or not self._registered
                or self.stopped_by_disconnect
            ):
                return
            self._stopping_announced = True
        try:
            self.bus.publish(
                "agent.stopping",
                {"name": self.name, "instance_id": self.instance_id},
                idempotency_key=f"stopping:{self.instance_id}",
            )
        except Exception as exc:
            self.log(
                f"[{self.name}] could not announce that it is stopping "
                f"({exc.__class__.__name__})"
            )

    def _announce_stop(self) -> None:
        """Tell the coordinator this process has stopped. Best effort, once.

        Without it the coordinator learns only when the lease runs out: until
        then it may hand this worker new work, and the work it held waits and
        is counted as a lost worker. With it, that work is handed back at
        once, and free of charge when the stop was requested.
        """
        with self._lock:
            if (
                self._stop_announced
                or not self._registered
                # The bus could not be reached: do not wait on it again.
                or self.stopped_by_disconnect
            ):
                return
            self._stop_announced = True
        try:
            self.bus.publish(
                "agent.stopped",
                {
                    "name": self.name,
                    "instance_id": self.instance_id,
                    "requested": self.exit_code == 0,
                },
                idempotency_key=f"stopped:{self.instance_id}",
            )
        except Exception as exc:
            self.log(
                f"[{self.name}] could not announce the stop ({exc.__class__.__name__}); "
                "the coordinator will notice when the lease expires"
            )

    def _stop_on_sigterm(self):
        """Treat SIGTERM like Ctrl-C while run() is in charge of the process.

        A supervisor stops a worker with SIGTERM, which by default ends
        Python at once: no cancellation of running agents, no announcement.
        Only done from the main thread and only when nobody else has
        installed a handler. Returns a callable that undoes it.
        """
        if threading.current_thread() is not threading.main_thread():
            return lambda: None
        try:
            if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
                return lambda: None

            def interrupt(signum, frame):
                raise KeyboardInterrupt

            signal.signal(signal.SIGTERM, interrupt)
        except (ValueError, OSError):
            return lambda: None

        def restore() -> None:
            try:
                if signal.getsignal(signal.SIGTERM) is interrupt:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
            except (ValueError, OSError):
                pass

        return restore

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
            self._registered = True
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

        restore_sigterm = self._stop_on_sigterm()
        completed_normally = False
        try:
            stream = events
            if stream is None:
                # A half-open SSE read must eventually notice stop_event. An
                # SSE timeout only reconnects; it does not arm the disconnect
                # timer, which counts failed writes.
                options = (
                    {"stream_timeout": STREAM_READ_TIMEOUT_SECONDS}
                    if self.disconnect_timeout_seconds is not None and isinstance(self.bus, BusClient)
                    else {}
                )
                stream = self.bus.subscribe(
                    topics=list(RUNTIME_TOPICS),
                    from_id=registered["id"],
                    stop_event=self.stop_event,
                    **options,
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
        except Exception as exc:
            # An unhandled stream/dispatch error is not an operator stop.
            # Record the cause before shutdown publishes the disposition.
            # KeyboardInterrupt (also used for SIGTERM) bypasses this handler.
            self._stopping_on_failure(exc)
            raise
        finally:
            try:
                self.shutdown(drain=completed_normally)
            finally:
                restore_sigterm()

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
                # The coordinator already knows: the new registration took
                # over the name, and a stop from this one would be ignored.
                with self._lock:
                    self._stop_announced = True
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
            self._stopping_on_failure(exc)
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
        """Ask the worker to stop. Returns at once; safe from any thread.

        No more work is accepted from this call on. Cancelling what is
        running, the wait for it, and the announcements to the coordinator
        happen on a background thread, so the work is handed back even if the
        caller, the event stream or an executor hook is blocked. Heartbeats
        continue until that is done (at most ATTEMPT_CANCEL_GRACE_SECONDS), so
        a caller must not assume executors have stopped when this returns:
        run() returning is what says so.
        """
        with self._lock:
            if self._closed and not self._registered:
                return  # A finished finite run has nothing left to hand back.
            if not self._registered:
                # Not running yet, so there is nothing to release. Leave the
                # hand-back to run(), which will register, see this and stop;
                # spending it here would leave that registration unannounced.
                self.stop_event.set()
                self._accepting = False
                return
        self._begin_release()

    def _begin_release(self) -> threading.Thread:
        # stop() may race run()'s finally block or be called by an adapter
        # hook. Give the handoff a single owner; never join it from stop().
        with self._lock:
            self.stop_event.set()
            self._accepting = False
            if self._release_thread is None:
                self._release_thread = threading.Thread(
                    target=self._release_work,
                    args=(tuple(self._owned), tuple(self._futures.values()),
                          time.monotonic() + ATTEMPT_CANCEL_GRACE_SECONDS),
                    name=f"{self.name}-release", daemon=True,
                )
                self._release_thread.start()
            return self._release_thread

    def _cleanup_executor(self, assignment_ids: tuple[str, ...]) -> None:
        for assignment_id in assignment_ids:
            self._revoke(assignment_id)
        try:
            self._close_executor()
        except Exception as exc:
            self.log(f"[{self.name}] executor cleanup failed ({exc.__class__.__name__}): {exc}")

    def _release_work(
        self, assignment_ids: tuple[str, ...], futures: tuple[Future, ...], deadline: float
    ) -> None:
        try:
            if any(not future.done() for future in futures):
                self._announce_stopping()
            else:
                # No execution to protect: an idle adapter's close() must not
                # leave the worker eligible for new assignments.
                self._heartbeat_stop.set()
                self._announce_stop()
            # cancel() and close() are adapter code. Keep them serialized, but
            # never let either own the clock that decides when the lease ends.
            cleanup = threading.Thread(target=self._cleanup_executor, args=(assignment_ids,),
                                       name=f"{self.name}-cleanup", daemon=True)
            cleanup.start()
            cleanup.join(max(0.0, deadline - time.monotonic()))
            running = [future for future in futures if not future.done()]
            if running:
                wait_for_futures(running, timeout=max(0.0, deadline - time.monotonic()))
            if cleanup.is_alive() or any(not future.done() for future in running):
                self.log(
                    f"[{self.name}] executor or cleanup hook still running "
                    f"{ATTEMPT_CANCEL_GRACE_SECONDS:g}s after cancellation; announcing the stop anyway"
                )
        finally:
            self._heartbeat_stop.set()
            self._announce_stop()  # Idempotent, including the idle path above.

    def shutdown(self, *, drain: bool = False) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._accepting = False
        if not drain:
            self._begin_release().join()
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=not drain)
        if drain:
            if self.stop_event.is_set():
                self._begin_release().join()
            else:
                self._close_executor()
        self.stop_event.set()
        self._stop_heartbeat()
        # A finite event iterable simply running out (embedding and tests) is
        # not announced: everything accepted was finished, so there is nothing
        # to hand back. A failure while draining is still a stop.
        if drain and self.exit_code != 0:
            self._announce_stop()
        with self._lock:
            self._registered = False

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        self._write_confirmed()
        heartbeat = self._heartbeat
        if heartbeat is not None and heartbeat is not threading.current_thread():
            heartbeat.join(timeout=max(1.0, self.heartbeat_seconds + 0.5))

    def _write_failed(self) -> None:
        """Arm once per outage. Further failures never extend the window."""
        timeout = self.disconnect_timeout_seconds
        if timeout is None:
            return
        with self._lock:
            if self._heartbeat_stop.is_set() or self._disconnect_timer is not None:
                return
            timer = threading.Timer(timeout, lambda: self._disconnect_elapsed(timer))
            timer.daemon = True
            self._disconnect_timer = timer
            timer.start()

    def _write_confirmed(self) -> None:
        if self.disconnect_timeout_seconds is not None:
            with self._lock:
                timer, self._disconnect_timer = self._disconnect_timer, None
                if timer is not None:
                    timer.cancel()

    def _disconnect_elapsed(self, timer: threading.Timer) -> None:
        with self._lock:
            # A successful write may already have disarmed this callback.
            if self._disconnect_timer is not timer or self.stop_event.is_set():
                return
            self._disconnect_timer = None
            self.stopped_by_disconnect = True
            self._accepting = False
            self.stop_event.set()
            self._heartbeat_stop.set()
            phases = dict(self._assignment_phases)
        self.log(
            f"[{self.name}] disconnect timeout ({self.disconnect_timeout_seconds:g}s); "
            f"stopping locally; unconfirmed assignments (id: phase): {phases}. "
            "Check bus history after recovery."
        )
        for assignment_id in phases:
            self._revoke(assignment_id)

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self.bus.publish(
                    "agent.heartbeat",
                    {"name": self.name, "instance_id": self.instance_id},
                )
                self._write_confirmed()
            except httpx.HTTPError as exc:
                refused = _refusal_status(exc)
                if refused is not None:
                    # The bus answered and said no (a bad token, say). That is
                    # not an outage to wait out: stop, as for a refused
                    # lifecycle write, so this worker neither keeps running
                    # work it cannot report nor idles while looking alive.
                    self.log(
                        f"[{self.name}] bus rejected agent.heartbeat with "
                        f"HTTP {refused}; stopping this instance"
                    )
                    self._stopping_on_failure(exc)
                    self.stop()
                    return
                self._write_failed()
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
                self._arm_attempt_timeout(assignment, lifecycle)
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
                # Read only now: the timer marks an attempt timed out while it
                # is still executing, which the transition above has ended.
                with self._lock:
                    timed_out = assignment_id in self._timed_out
                if timed_out:
                    # Once the limit has passed the attempt is a timeout,
                    # whatever the cancelled executor handed back.
                    outcome = self._attempt_timeout_outcome()

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

            self._publish_attempt_failed(assignment, lifecycle, outcome)
        finally:
            with self._lock:
                abandoned = assignment_id in self._abandoned
            # An abandoned attempt is being reported by _attempt_abandoned,
            # which still needs the ownership record this would remove.
            if not abandoned:
                self._forget_assignment(assignment_id)

    def _publish_attempt_failed(
        self,
        assignment: AssignmentContext,
        lifecycle: dict,
        outcome: Union[RetryableFailure, PermanentFailure],
        *,
        give_up_at: Optional[float] = None,
    ) -> Optional[dict]:
        retryable = isinstance(outcome, RetryableFailure)
        return self._publish_while_owned(
            assignment.assignment_id,
            "task.attempt_failed",
            {
                **lifecycle,
                "failure_code": outcome.code,
                "reason": outcome.reason,
                "retryable": retryable,
                **(
                    {"retry_after_seconds": outcome.retry_after_seconds}
                    if isinstance(outcome, RetryableFailure)
                    and outcome.retry_after_seconds is not None
                    else {}
                ),
            },
            caused_by=assignment.assignment_event_id,
            idempotency_key=f"attempt-failed:{assignment.assignment_id}",
            give_up_at=give_up_at,
        )

    def _forget_assignment(self, assignment_id: str) -> None:
        """Drop every record of an assignment this worker is finished with."""
        with self._lock:
            self._owned.discard(assignment_id)
            self._assignment_phases.pop(assignment_id, None)
            self._futures.pop(assignment_id, None)
            self._task_by_assignment.pop(assignment_id, None)
            self._correlation_by_assignment.pop(assignment_id, None)
            self._timed_out.discard(assignment_id)
            timers = (
                self._deadline_timers.pop(assignment_id, None),
                self._attempt_timers.pop(assignment_id, None),
            )
        for timer in timers:
            if timer is not None:
                timer.cancel()

    def _attempt_timeout_outcome(self) -> RetryableFailure:
        return RetryableFailure(
            "attempt_timeout",
            f"attempt ran longer than this worker's "
            f"{self.attempt_timeout_seconds:g}-second limit",
        )

    def _arm_attempt_timeout(self, assignment: AssignmentContext, lifecycle: dict) -> None:
        if self.attempt_timeout_seconds is None:
            return
        timer = threading.Timer(
            self.attempt_timeout_seconds,
            self._attempt_timeout_elapsed,
            args=(assignment, lifecycle),
        )
        timer.daemon = True
        with self._lock:
            if assignment.assignment_id not in self._owned:
                return
            self._attempt_timers[assignment.assignment_id] = timer
        timer.start()

    def _attempt_timeout_elapsed(self, assignment: AssignmentContext, lifecycle: dict) -> None:
        """The limit has passed: cancel the executor and give it a grace period."""
        assignment_id = assignment.assignment_id
        grace = threading.Timer(
            ATTEMPT_CANCEL_GRACE_SECONDS,
            self._attempt_abandoned,
            args=(assignment, lifecycle),
        )
        grace.daemon = True
        with self._lock:
            if (
                self.stop_event.is_set()
                or assignment_id not in self._owned
                or self._assignment_phases.get(assignment_id) != _PHASE_EXECUTING
            ):
                return
            self._timed_out.add(assignment_id)
            self._attempt_timers[assignment_id] = grace
        self.log(
            f"[{self.name}] attempt timeout ({self.attempt_timeout_seconds:g}s) "
            f"reached: {assignment_id}; cancelling"
        )
        # Started first: a cancel() that blocks must not postpone the grace.
        grace.start()
        self._cancel_executor(assignment_id)

    def _attempt_abandoned(self, assignment: AssignmentContext, lifecycle: dict) -> None:
        """The executor ignored cancellation: retire this worker.

        Its thread is still inside execute(), so the slot cannot be reused. A
        worker that kept heartbeating would be handed assignments it could
        never start. So it takes no more work and renews its lease no further
        from this moment, tries for a bounded time to report the timeout, and
        stops whether or not that got through: the lease running out frees
        its tasks if nothing else does.
        """
        assignment_id = assignment.assignment_id
        with self._lock:
            if (
                self.stop_event.is_set()
                or assignment_id not in self._owned
                or self._assignment_phases.get(assignment_id) != _PHASE_EXECUTING
            ):
                return
            # From here the executor's eventual return is discarded.
            self._assignment_phases[assignment_id] = _PHASE_PUBLISHING_OUTCOME
            self._abandoned.add(assignment_id)
            self._accepting = False
        self._heartbeat_stop.set()
        # First, so the retry of this very task is not sent back here.
        self._announce_stopping()
        self.log(
            f"[{self.name}] executor did not stop within "
            f"{ATTEMPT_CANCEL_GRACE_SECONDS:g}s of cancellation: {assignment_id}; "
            "reporting the timeout and stopping this worker, because the slot "
            "it occupies cannot be reclaimed"
        )
        reported = self._publish_attempt_failed(
            assignment,
            lifecycle,
            self._attempt_timeout_outcome(),
            give_up_at=time.monotonic() + ATTEMPT_REPORT_SECONDS,
        )
        if reported is None:
            self.log(
                f"[{self.name}] could not report the timeout for {assignment_id}; "
                "the coordinator will recover it when this worker's lease expires"
            )
        self._forget_assignment(assignment_id)
        with self._lock:
            self._abandoned.discard(assignment_id)
        self._stopping_on_failure()
        self.stop()

    def _publish_while_owned(
        self,
        assignment_id: str,
        topic: str,
        payload: dict,
        *,
        caused_by: int,
        idempotency_key: str,
        give_up_at: Optional[float] = None,
    ) -> Optional[dict]:
        """Publish, retrying transient failures for as long as it is ours.

        give_up_at (a time.monotonic() value) bounds the retries for a caller
        that must not wait on the bus indefinitely.
        """
        while self._owns(assignment_id) and not self.stop_event.is_set():
            try:
                sent = self.bus.publish(
                    topic,
                    payload,
                    caused_by=caused_by,
                    idempotency_key=idempotency_key,
                )
                self._write_confirmed()
                return sent
            except httpx.HTTPStatusError as exc:
                if 400 <= exc.response.status_code < 500:
                    self.log(
                        f"[{self.name}] bus rejected {topic} with "
                        f"HTTP {exc.response.status_code}; stopping this instance"
                    )
                    self._stopping_on_failure(exc)
                    self.stop_event.set()
                    return None
                self._write_failed()
                if give_up_at is not None and time.monotonic() >= give_up_at:
                    break
                self.log(
                    f"[{self.name}] publish failed ({exc.__class__.__name__}); "
                    f"retrying {topic}"
                )
                if self.stop_event.wait(self.publish_retry_seconds):
                    break
            except httpx.HTTPError as exc:
                self._write_failed()
                if give_up_at is not None and time.monotonic() >= give_up_at:
                    break
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
                self.stop_event.is_set()
                or assignment_id not in self._owned
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
            timers = (
                self._deadline_timers.pop(assignment_id, None),
                self._attempt_timers.pop(assignment_id, None),
            )
        for timer in timers:
            if timer is not None:
                timer.cancel()
        cancelled_before_start = False
        if future is not None:
            cancelled_before_start = future.cancel()
            if cancelled_before_start:
                with self._lock:
                    self._futures.pop(assignment_id, None)
        if not cancelled_before_start and phase == _PHASE_EXECUTING:
            self._cancel_executor(assignment_id)

    def _cancel_executor(self, assignment_id: str) -> None:
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
                if self._executor_closed:
                    return
                self._executor_closed = True
                close()
