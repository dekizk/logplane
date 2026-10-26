"""Reference worker built on the reusable v0.4 executor runtime."""

import argparse
import os
import threading
import time

from logplane.legacy import check_legacy_environment
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    PermanentFailure,
    RetryableFailure,
)

BUS_URL = os.environ.get("LOGPLANE_URL", "http://127.0.0.1:8765")
HEARTBEAT_SECONDS = float(os.environ.get("LOGPLANE_HEARTBEAT_SECONDS", "5"))


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

    def execute(self, assignment: AssignmentContext):
        # Replace this body with a real agent implementation. The surrounding
        # runtime already owns registration, leases, concurrency, and events.
        time.sleep(self.delay_seconds)
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


def parse_args(argv=None) -> argparse.Namespace:
    from logplane.cli import add_demo_worker_arguments

    parser = argparse.ArgumentParser(description="Run a demo logplane worker")
    add_demo_worker_arguments(parser)
    return parser.parse_args(argv)


def run_demo_worker(args: argparse.Namespace, *, bus_url: str,
                    heartbeat_seconds: float = 5.0) -> None:
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
    )
    runtime.run()


def main() -> None:
    """Compatibility entry point: keep the environment-only launch supported."""
    check_legacy_environment()
    run_demo_worker(parse_args(), bus_url=BUS_URL, heartbeat_seconds=HEARTBEAT_SECONDS)


if __name__ == "__main__":
    main()
