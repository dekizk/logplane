"""One explicitly approved paid OpenClaw task, through a disposable live bus."""

import argparse
import json
import sys
import tempfile
import threading
from pathlib import Path

from examples.openclaw.openclaw_executor import OpenClawExecutor
from logplane.client import BusClient
from logplane.demo import Harness
from logplane.projection import replay_events
from logplane.runtime import WorkerRuntime


def verify_trial(events: list[dict], task_id: int, report: dict) -> bool:
    """Verify lifecycle/result; absent optional tool counts remain unknown.

    The executor independently requires a deny-all tool policy. OpenClaw's
    agent-exec toolSummary is additive: missing does not mean a measured zero.
    A positive reported count contradicts this pilot's expected policy.
    """
    replayed = replay_events(events)
    completed = [e for e in events if e["topic"] == "task.completed" and e["payload"]["task_id"] == task_id]
    assignments = [e for e in events if e["topic"] == "task.assigned" and e["payload"]["task_id"] == task_id]
    calls = report.get("toolSummary", {}).get("calls")
    return (len(assignments) == 1 and len(completed) == 1
            and completed[0]["payload"]["result"] == {"value": 42}
            and replayed.tasks[task_id].status == "completed"
            and (calls is None or type(calls) is int and calls == 0))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="approve one paid assignment")
    parser.add_argument("--openclaw-command", default="openclaw")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("nous-proxy.json"))
    parser.add_argument("--model", default="nousportal/openai/gpt-6-luna")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", type=Path, help="new directory for non-secret trial evidence")
    args = parser.parse_args()
    if not args.yes:
        parser.error("pass --yes to approve one paid model assignment (no automatic task retries)")
    if args.output:
        root = args.output.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=False)
    else:
        root = Path(tempfile.mkdtemp(prefix="logplane-openclaw-evidence-"))
    workspace = root / "workspace"
    workspace.mkdir()
    executor = OpenClawExecutor(command=(args.openclaw_command,), config=args.config,
                                model=args.model, working_directory=workspace,
                                timeout=args.timeout)
    harness = Harness(root, out=sys.stdout, lease_seconds=30)
    runtime = WorkerRuntime(BusClient(harness.url, actor="openclaw"), name="openclaw",
                            executor=executor, capacity=1, capabilities=["openclaw"],
                            heartbeat_seconds=2)
    thread = threading.Thread(target=runtime.run, daemon=True)
    evidence = {"passed": False, "requested_model": args.model}
    try:
        harness.start_bus()
        harness.start_pm()
        thread.start()
        harness.wait_for("agent.registered", actor="openclaw", timeout=20)
        task_id, correlation_id = harness.submit(
            'Return a completed outcome with summary "OpenClaw worker test passed" '
            'and result {"value":42}. Do not use tools.',
            capability="openclaw", max_retries=0, quiet=True,
        )
        harness.wait_until(
            lambda: harness.task_state(task_id).get("status") in {"completed", "failed", "blocked"},
            args.timeout + 30, "the OpenClaw outcome",
        )
        events = harness.events()
        (root / "events.json").write_text(json.dumps(events, indent=2) + "\n")
        replayed = replay_events(events)
        assignments = [e for e in events if e["topic"] == "task.assigned" and e["payload"]["task_id"] == task_id]
        report = executor.last_report[1] if executor.last_report else {}
        evidence.update(task_id=task_id, correlation_id=correlation_id,
                        status=replayed.tasks[task_id].status,
                        assignments=len(assignments), openclaw_report=report,
                        tool_policy="deny all; plugins disabled",
                        reported_tool_calls=report.get("toolSummary", {}).get("calls"),
                        lifecycle=[{"id": e["id"], "topic": e["topic"], "caused_by": e.get("caused_by")}
                                   for e in events if e["payload"].get("task_id") == task_id])
        passed = verify_trial(events, task_id, report)
        evidence["passed"] = passed
        print(json.dumps(evidence, indent=2))
        if not passed:
            raise RuntimeError(f"trial did not meet acceptance criteria; inspect {root}")
    finally:
        runtime.stop()
        executor.close()
        if thread.ident is not None:
            thread.join(timeout=5)
        harness.stop_all()
        (root / "summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(f"Evidence: {root}")


if __name__ == "__main__":
    main()
