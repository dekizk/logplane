"""Reference worker built on the reusable v0.4 executor runtime."""

import argparse
import os
import threading

from logplane.legacy import check_legacy_environment
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    PermanentFailure,
    RetryableFailure,
)

BUS_URL = os.environ.get("LOGPLANE_URL", "http://127.0.0.1:8765")
HEARTBEAT_ENV = "LOGPLANE_HEARTBEAT_SECONDS"
HEARTBEAT_SECONDS = float(os.environ.get(HEARTBEAT_ENV, "5"))


def configured_heartbeat_seconds() -> float:
    """The demo worker's heartbeat interval: LOGPLANE_HEARTBEAT_SECONDS, else 5.

    Read when the worker starts, so `logplane demo-worker` and
    `python -m logplane.worker` always agree.
    """
    raw = os.environ.get(HEARTBEAT_ENV)
    if raw is None:
        return 5.0
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    if not 0 < value < float("inf"):
        raise ValueError(f"{HEARTBEAT_ENV} must be a positive number of seconds")
    return value


class DemoExecutor:
    """Small reference executor used by the command-line demonstration."""

    def __init__(
        self,
        name: str,
        *,
        block_once: int | None = None,
        fail_once: int | None = None,
        fail_permanently_once: int | None = None,
        delay_seconds: float = 0.5,
    ):
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        self.name = name
        self.delay_seconds = delay_seconds
        self.block_once = block_once
        self.fail_once = fail_once
        self.fail_permanently_once = fail_permanently_once
        self._lock = threading.Lock()
        self._cancelled: dict[str, threading.Event] = {}

    def execute(self, assignment: AssignmentContext):
        # Replace this body with a real agent implementation. The surrounding
        # runtime already owns registration, leases, concurrency, and events.
        cancelled = threading.Event()
        with self._lock:
            self._cancelled[assignment.assignment_id] = cancelled
        try:
            # The simulated work. A real agent should give up just as promptly
            # when cancel() is called: that is what lets the runtime stop a
            # revoked, timed-out or shut-down attempt and reuse the slot.
            if cancelled.wait(self.delay_seconds):
                return RetryableFailure(
                    "demo_cancelled",
                    "demo worker was told to stop this task",
                )
        finally:
            with self._lock:
                self._cancelled.pop(assignment.assignment_id, None)
        with self._lock:
            if self.fail_once == assignment.task_id:
                self.fail_once = None
                return RetryableFailure(
                    "demo_retryable_failure",
                    "demo worker was asked to fail this task",
                )
            if self.fail_permanently_once == assignment.task_id:
                self.fail_permanently_once = None
                return PermanentFailure(
                    "demo_permanent_failure",
                    "demo worker was asked to fail this task",
                )
            if self.block_once == assignment.task_id:
                self.block_once = None
                return Blocked("Choose storage backend: SQLite or Postgres")
        return Completed(
            summary=f"{self.name} finished: {assignment.goal}",
        )

    def cancel(self, assignment_id: str) -> None:
        with self._lock:
            cancelled = self._cancelled.get(assignment_id)
        if cancelled is not None:
            cancelled.set()


def parse_args(argv=None) -> argparse.Namespace:
    from logplane.cli import add_demo_worker_arguments

    parser = argparse.ArgumentParser(description="Run a demo logplane worker")
    add_demo_worker_arguments(parser)
    return parser.parse_args(argv)


def run_demo_worker(args: argparse.Namespace, *, bus_url: str,
                    heartbeat_seconds: float | None = None) -> int:
    """Run until stopped; return the process exit status (see exit_code)."""
    if heartbeat_seconds is None:
        heartbeat_seconds = configured_heartbeat_seconds()
    from logplane.client import BusClient
    from logplane.runtime import WorkerRuntime

    executor = DemoExecutor(
        args.name,
        block_once=args.block,
        fail_once=args.fail,
        fail_permanently_once=args.fail_permanently,
        delay_seconds=args.delay,
    )
    bus = BusClient(bus_url, actor=args.name)
    runtime = WorkerRuntime(
        bus,
        name=args.name,
        executor=executor,
        capacity=args.capacity,
        capabilities=args.capabilities,
        heartbeat_seconds=heartbeat_seconds,
        attempt_timeout_seconds=args.attempt_timeout,
    )
    runtime.run()
    return runtime.exit_code


def main() -> None:
    """Compatibility entry point: keep the environment-only launch supported."""
    check_legacy_environment()
    status = run_demo_worker(parse_args(), bus_url=BUS_URL)
    if status:
        raise SystemExit(status)


if __name__ == "__main__":
    main()
