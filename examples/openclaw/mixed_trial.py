"""A real Hermes -> OpenClaw DAG, with two explicitly approved paid assignments."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
import uuid
from pathlib import Path

from examples.hermes.hermes_executor import HermesExecutor
from examples.openclaw.openclaw_executor import OpenClawExecutor
from logplane.client import BusClient
from logplane.demo import Harness
from logplane.projection import replay_events
from logplane.runtime import WorkerRuntime


class RecordingExecutor:
    """Record exactly what the runtime handed to the adapter, for this trial."""

    def __init__(self, delegate):
        self.delegate = delegate
        self.assignments: list[dict] = []

    def execute(self, assignment):
        self.assignments.append(assignment.to_dict())
        return self.delegate.execute(assignment)

    def cancel(self, assignment_id):
        self.delegate.cancel(assignment_id)

    def close(self):
        self.delegate.close()


def verify_handoff(events: list[dict], producer_id: int, consumer_id: int,
                   expected: dict, deliveries: list[dict]) -> dict[str, bool]:
    def one(topic, task_id):
        matches = [event for event in events
                   if event["topic"] == topic and event["payload"].get("task_id") == task_id]
        if len(matches) != 1:
            raise ValueError(f"expected exactly one {topic} for task {task_id}, got {len(matches)}")
        return matches[0]

    parent_created = one("task.created", producer_id)
    child_created = one("task.created", consumer_id)
    parent_assigned = one("task.assigned", producer_id)
    child_assigned = one("task.assigned", consumer_id)
    parent_done = one("task.completed", producer_id)
    child_done = one("task.completed", consumer_id)
    parent_started = one("task.started", producer_id)
    child_started = one("task.started", consumer_id)
    state = replay_events(events)
    reference = {"task_id": producer_id, "completion_event_id": parent_done["id"]}
    resolved = {**reference, "summary": parent_done["payload"]["summary"], "result": expected}
    expected_child = {"handoff_marker": expected["handoff_marker"], "total": sum(expected["numbers"]),
                      "source_task_id": producer_id, "source_completion_event_id": parent_done["id"]}
    return {
        "both_tasks_completed_on_replay": all(state.tasks[i].status == "completed" for i in (producer_id, consumer_id)),
        "both_created_before_execution": child_created["id"] < parent_assigned["id"],
        "one_shared_workflow": len({e["correlation_id"] for e in (
            parent_created, child_created, parent_assigned, child_assigned, parent_done, child_done,
        )}) == 1 and bool(parent_created["correlation_id"]),
        "immutable_dependency_declared": child_created["payload"].get("depends_on") == [producer_id],
        "child_not_assigned_early": parent_done["id"] < child_assigned["id"] < child_started["id"] < child_done["id"],
        "correct_workers": (parent_assigned["payload"]["assignee"] == parent_done["actor"] == "hermes"
                            and child_assigned["payload"]["assignee"] == child_done["actor"] == "openclaw"),
        "lifecycle_causes_preserved": all(started["caused_by"] == done["caused_by"] == assigned["id"]
                                         for started, done, assigned in (
                                             (parent_started, parent_done, parent_assigned),
                                             (child_started, child_done, child_assigned))),
        "producer_result_correct": parent_done["payload"].get("result") == expected,
        "consumer_result_correct": child_done["payload"].get("result") == expected_child,
        "no_manual_result_copy": child_created["payload"].get("context") == child_assigned["payload"].get("context") == {},
        "assignment_uses_completion_reference": child_assigned["payload"].get("dependency_refs") == [reference],
        "runtime_resolved_result": (len(deliveries) == 1 and deliveries[0].get("task_id") == consumer_id
                                    and deliveries[0].get("dependencies") == [resolved]),
        "no_failure_or_expiry": not any(e["topic"] in {
            "task.attempt_failed", "task.assignment_expired", "task.failed", "task.dependency_failed",
        } for e in events),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="approve two paid assignments")
    parser.add_argument("--hermes-command", default="hermes")
    parser.add_argument("--openclaw-command", default="openclaw")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("nous-proxy.json"))
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, help="new directory for retained trial evidence")
    args = parser.parse_args()
    if not args.yes:
        parser.error("pass --yes to approve two paid assignments (zero automatic task retries)")
    if args.output:
        root = args.output.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=False)
    else:
        root = Path(tempfile.mkdtemp(prefix="logplane-mixed-trial-"))
    for name in ("hermes", "openclaw"):
        (root / name).mkdir()
    expected = {"handoff_marker": uuid.uuid4().hex, "numbers": [17, 25, 9]}
    hermes_usage = []
    hermes = RecordingExecutor(HermesExecutor(
        command=(args.hermes_command,), working_directory=root / "hermes",
        provider="nous", model="openai/gpt-6-luna", reasoning="high",
        toolsets=("clarify",), safe_mode=True, timeout=args.timeout,
        usage_callback=lambda assignment_id, usage: hermes_usage.append(
            {"assignment_id": assignment_id, **dict(usage)}),
    ))
    claw = OpenClawExecutor(command=(args.openclaw_command,), config=args.config,
                           working_directory=root / "openclaw", model="nousportal/openai/gpt-6-luna",
                           thinking="high", timeout=args.timeout)
    openclaw = RecordingExecutor(claw)
    harness = Harness(root, out=sys.stdout, lease_seconds=30)
    runtimes = {name: WorkerRuntime(
        BusClient(harness.url, actor=name, offset_dir=root / ".offsets"),
        name=name, executor=executor, capacity=1, capabilities=[name], heartbeat_seconds=2,
    ) for name, executor in (("hermes", hermes), ("openclaw", openclaw))}
    threads = {name: threading.Thread(target=runtime.run, daemon=True) for name, runtime in runtimes.items()}
    evidence: dict = {"passed": False, "expected_producer_result": expected}
    events = []
    try:
        harness.start_bus()
        harness.start_pm()
        producer, correlation = harness.submit(
            "Return a completed outcome. Its result must contain exactly the handoff_marker "
            "and numbers from your supplied context, unchanged. No tools are needed.",
            context=expected, capability="hermes", max_retries=0, quiet=True,
        )
        consumer, _ = harness.submit(
            "Read the single entry in your dependencies array. Return a completed outcome with "
            "result containing exactly: handoff_marker (copy upstream result.handoff_marker), "
            "total (sum upstream result.numbers), source_task_id (the dependency task_id), "
            "and source_completion_event_id (the dependency completion_event_id). "
            "Use only the provided dependency, not guessed values. No tools are needed.",
            depends_on=[producer], context={}, capability="openclaw", max_retries=0,
            correlation_id=correlation, quiet=True,
        )
        evidence.update(producer_task_id=producer, consumer_task_id=consumer, correlation_id=correlation)
        # The consumer is online before any producer can execute. Record its
        # waiting state before starting Hermes, rather than copying output later.
        threads["openclaw"].start()
        harness.wait_for("agent.registered", actor="openclaw", timeout=20)
        evidence["consumer_before_producer_started"] = harness.task_state(consumer)
        if evidence["consumer_before_producer_started"]["status"] != "open":
            raise RuntimeError("consumer did not wait for its dependency")
        threads["hermes"].start()
        harness.wait_for("agent.registered", actor="hermes", timeout=20)
        harness.wait_until(lambda: all(harness.task_state(i)["status"] in {
            "completed", "failed", "blocked", "dependency_failed", "cancelled",
        } for i in (producer, consumer)), 2 * args.timeout + 45, "both agent outcomes")
        events = harness.events()
        evidence["checks"] = verify_handoff(events, producer, consumer, expected, openclaw.assignments)
        evidence["passed"] = all(evidence["checks"].values())
        evidence["workflow"] = harness.workflow_state(correlation)
        print(json.dumps({"passed": evidence["passed"], "checks": evidence["checks"]}, indent=2))
        if not evidence["passed"]:
            raise RuntimeError("handoff verification failed; inspect retained evidence")
    except Exception as exc:
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        for runtime in runtimes.values():
            runtime.stop()
        for executor in (hermes, openclaw):
            executor.close()
        for thread in threads.values():
            if thread.ident is not None:
                thread.join(timeout=5)
        try:
            if "bus" in harness.processes:
                events = harness.events()
        finally:
            harness.stop_all()
            evidence["hermes_usage"] = hermes_usage
            evidence["openclaw_report"] = claw.last_report[1] if claw.last_report else None
            for name, value in (("summary", evidence), ("events", events),
                                ("deliveries", {"hermes": hermes.assignments, "openclaw": openclaw.assignments})):
                (root / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
            print(f"Evidence: {root}")


if __name__ == "__main__":
    main()
