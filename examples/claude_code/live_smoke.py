"""One explicitly approved Claude Code task, through a disposable live bus.

Starts a separate bus and coordinator, runs a Claude Code worker with the auth
mode you choose, submits one task returning {"value": 42}, and checks the
result, the replayed state, and that the call's usage reached Logplane's
workflow accounting (so workflow budgets can see it).
"""

import argparse
import json
import sys
import tempfile
import threading
import uuid
from pathlib import Path

from examples.claude_code.claude_code_executor import AUTH_MODES, ClaudeCodeExecutor
from logplane.client import BusClient
from logplane.demo import Harness
from logplane.projection import replay_events
from logplane.runtime import WorkerRuntime
from logplane.telemetry import BusTelemetrySink, ProducerIdentity


def verify_smoke(events: list[dict], task_id: int) -> dict[str, bool]:
    replayed = replay_events(events)
    mine = [e for e in events if e["payload"].get("task_id") == task_id]
    assignments = [e for e in mine if e["topic"] == "task.assigned"]
    completed = [e for e in mine if e["topic"] == "task.completed"]
    accounted = [e for e in mine if e["topic"] == "workflow.usage_recorded"]
    return {
        "one_attempt": len(assignments) == 1,
        "completed_with_expected_result": (len(completed) == 1
                                           and completed[0]["payload"].get("result") == {"value": 42}),
        "completed_on_replay": replayed.tasks[task_id].status == "completed",
        "usage_reached_accounting": (len(accounted) == 1
                                     and isinstance(accounted[0]["payload"].get("cost_usd"), (int, float))
                                     and accounted[0]["payload"]["cost_usd"] > 0
                                     and isinstance(accounted[0]["payload"].get("tokens"), int)
                                     and accounted[0]["payload"]["tokens"] > 0),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="approve one paid assignment")
    parser.add_argument("--auth", required=True, choices=AUTH_MODES)
    parser.add_argument("--claude-command", default="claude")
    parser.add_argument("--model", help="defaults to Claude Code's own default")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, help="new directory for retained trial evidence")
    args = parser.parse_args(argv)
    if not args.yes:
        parser.error("pass --yes to approve one paid assignment (no automatic task retries)")
    if args.output:
        root = args.output.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=False)
    else:
        root = Path(tempfile.mkdtemp(prefix="logplane-claude-evidence-"))
    workspace = root / "workspace"
    workspace.mkdir()
    harness = Harness(root, out=sys.stdout, lease_seconds=30)
    bus = BusClient(harness.url, actor="claude")
    instance_id = uuid.uuid4().hex
    executor = ClaudeCodeExecutor(
        auth=args.auth, working_directory=workspace, command=(args.claude_command,),
        model=args.model, timeout=args.timeout, max_budget_usd=0.25,
        telemetry_sink=BusTelemetrySink(bus, producer=ProducerIdentity(
            "examples.claude_code", instance_id, "0.1.0")),
    )
    runtime = WorkerRuntime(bus, name="claude", executor=executor, instance_id=instance_id,
                            capacity=1, capabilities=["claude"], heartbeat_seconds=2)
    thread = threading.Thread(target=runtime.run, daemon=True)
    evidence: dict = {"passed": False, "auth": args.auth, "requested_model": args.model}
    events: list[dict] = []
    try:
        harness.start_bus()
        harness.start_pm()
        thread.start()
        harness.wait_for("agent.registered", actor="claude", timeout=20)
        task_id, correlation_id = harness.submit(
            'Return a completed outcome with summary "Claude Code worker test passed" '
            'and result {"value":42}.',
            capability="claude", max_retries=0, quiet=True,
        )
        harness.wait_until(
            lambda: harness.task_state(task_id).get("status") in {"completed", "failed", "blocked"},
            args.timeout + 30, "the Claude Code outcome",
        )
        harness.wait_until(  # usage is recorded just after the outcome
            lambda: harness.find_event("workflow.usage_recorded", task_id=task_id), 10,
            "usage accounting")
        events = harness.events()
        checks = verify_smoke(events, task_id)
        report = executor.last_report[1] if executor.last_report else {}
        evidence.update(task_id=task_id, correlation_id=correlation_id, checks=checks,
                        claude_report=report, workflow=harness.workflow_state(correlation_id),
                        passed=all(checks.values()))
        print(json.dumps({"passed": evidence["passed"], "checks": checks, "claude_report": report},
                         indent=2))
        if not evidence["passed"]:
            raise RuntimeError(f"trial did not meet acceptance criteria; inspect {root}")
    except Exception as exc:
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        if executor.last_report:
            evidence["claude_report"] = executor.last_report[1]
        raise
    finally:
        runtime.stop()
        executor.close()
        if thread.ident is not None:
            thread.join(timeout=5)
        try:
            if not events and "bus" in harness.processes:
                events = harness.events()
        finally:
            harness.stop_all()
            (root / "events.json").write_text(json.dumps(events, indent=2) + "\n")
            (root / "summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
            print(f"Evidence: {root}")


if __name__ == "__main__":
    main()
