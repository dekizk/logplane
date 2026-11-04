"""Start the optional text-only OpenClaw worker against an existing bus."""

import argparse
import json
import os
import threading
import time
from pathlib import Path

from examples.openclaw.openclaw_executor import OpenClawExecutor
from logplane.client import BusClient
from logplane.executors import Completed, mutable_json
from logplane.runtime import WorkerRuntime


def outcome_record(outcome) -> dict:
    """A JSON description of an executor outcome, for trial evidence."""
    record = {"type": type(outcome).__name__}
    for name in ("summary", "reason", "code"):
        if hasattr(outcome, name):
            record[name] = getattr(outcome, name)
    if isinstance(outcome, Completed):
        record["result"] = mutable_json(outcome.result)
    return record


class RecordingExecutor:
    """Append what the runtime handed the adapter, and what it returned.

    Used by trials to show what happened inside a worker process, including
    outcomes the runtime then discarded because the assignment was no longer
    owned. Records hold assignment inputs and agent outcomes, not credentials.
    """

    def __init__(self, delegate, path: Path):
        self.delegate = delegate
        self.path = path
        self._lock = threading.Lock()

    def _append(self, entry: dict) -> None:
        line = json.dumps({"t": time.time(), "pid": os.getpid(), **entry}, sort_keys=True)
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def execute(self, assignment):
        self._append({"event": "received", "assignment": assignment.to_dict()})
        outcome = self.delegate.execute(assignment)
        entry = {"event": "returned", "assignment_id": assignment.assignment_id,
                 "outcome": outcome_record(outcome)}
        # OpenClaw-reported usage and cost, kept apart from the agent's result.
        report = getattr(self.delegate, "last_report", None)
        if report and report[0] == assignment.assignment_id:
            entry["openclaw_report"] = report[1]
        self._append(entry)
        return outcome

    def cancel(self, assignment_id):
        self.delegate.cancel(assignment_id)

    def close(self):
        self.delegate.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bus-url", default="http://127.0.0.1:8765")
    parser.add_argument("--name", default="openclaw")
    parser.add_argument("--openclaw-command", default="openclaw")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--working-directory", required=True, type=Path)
    parser.add_argument("--model", default="nousportal/openai/gpt-6-luna")
    parser.add_argument("--thinking", default="high")
    parser.add_argument("--timeout", default=120.0, type=float)
    parser.add_argument("--heartbeat-seconds", default=2.0, type=float)
    parser.add_argument("--record", type=Path,
                        help="append received assignments and returned outcomes to this JSONL file")
    args = parser.parse_args(argv)
    executor = OpenClawExecutor(
        command=(args.openclaw_command,), config=args.config, model=args.model,
        working_directory=args.working_directory, timeout=args.timeout, thinking=args.thinking,
    )
    worker_executor = RecordingExecutor(executor, args.record) if args.record else executor
    runtime = WorkerRuntime(BusClient(args.bus_url, actor=args.name), name=args.name,
                            executor=worker_executor, capacity=1, capabilities=["openclaw"],
                            heartbeat_seconds=args.heartbeat_seconds)
    try:
        runtime.run()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", flush=True)
    finally:
        executor.close()


if __name__ == "__main__":
    main()
