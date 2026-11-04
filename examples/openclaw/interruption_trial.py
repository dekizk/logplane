"""Hermes -> OpenClaw with real interruptions: a crashed worker and a frozen one.

Each worker is its own process. Task 2 (OpenClaw) depends on task 1 (Hermes).
OpenClaw worker A is killed mid-task, together with the agent it started.
Worker B takes the retry and is frozen (SIGSTOP) mid-task until its lease
expires and worker C finishes the task; B is then resumed, so it holds a genuine
late result for an attempt it no longer owns. The trial checks that Logplane
expired both attempts, delivered the same dependency to every attempt, and
accepted only worker C's result.

Whether B's runtime discards its late result, or cancels its adapter first, is
a race; both are correct. To exercise the coordinator's own guarantee as well,
the trial then sends attempt 2's completion exactly as a buggy or cut-off worker
would (a labelled probe, no model call) and checks that replay ignores it.

Paid: one Hermes call and up to three OpenClaw calls (the crashed one may be
cut short). Every agent runs with tools disabled.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from logplane.client import BusClient
from logplane.demo import Harness
from logplane.projection import PMState, apply_event, replay_events
from logplane.topics import COORDINATION_TOPICS

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTCOME_TOPICS = {"task.completed", "task.attempt_failed", "task.blocked"}
PRODUCER_GOAL = (
    "Return a completed outcome. Its result must contain exactly the handoff_marker "
    "and numbers from your supplied context, unchanged. No tools are needed."
)
CONSUMER_GOAL = (
    "Read the single entry in your dependencies array. Return a completed outcome with "
    "result containing exactly: handoff_marker (copy upstream result.handoff_marker), "
    "total (sum upstream result.numbers), source_task_id (the dependency task_id), "
    "and source_completion_event_id (the dependency completion_event_id). "
    "Use only the provided dependency, not guessed values. No tools are needed."
)


def read_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def rejected_events(events: list[dict]) -> list[dict]:
    """Coordination events the reducer ignored on replay, in log order.

    Telemetry and other non-coordination topics are not replayed at all, so
    they are left out rather than counted as ignored.
    """
    state = PMState()
    return [event for event in events
            if event["topic"] in COORDINATION_TOPICS and not apply_event(state, event)]


def verify_interruption(events: list[dict], producer_id: int, consumer_id: int, expected: dict,
                        records: dict[str, list[dict]], workers: tuple[str, str, str],
                        agent_processes_killed: int,
                        probe_event_id: int | None) -> tuple[dict[str, bool], dict]:
    """Acceptance checks, plus facts worth reporting (which layer stopped the late result)."""
    crashed, frozen, finisher = workers

    def task_events(topic, task_id):
        return [e for e in events if e["topic"] == topic and e["payload"].get("task_id") == task_id]

    def by_assignment(topics, assignment_id):
        return [e for e in events if e["topic"] in topics
                and e["payload"].get("assignment_id") == assignment_id]

    assigned = task_events("task.assigned", consumer_id)
    ids = [e["payload"]["assignment_id"] for e in assigned]
    expired = {e["payload"]["assignment_id"]: e for e in task_events("task.assignment_expired", consumer_id)}
    producer_done = task_events("task.completed", producer_id)
    state = replay_events(events)
    rejected_ids = {e["id"] for e in rejected_events(events)}

    three_attempts = (len(assigned) == 3
                      and [e["payload"]["assignee"] for e in assigned] == list(workers)
                      and [e["payload"]["attempt"] for e in assigned] == [1, 2, 3])
    if not (three_attempts and len(producer_done) == 1):
        # Without this shape the remaining checks have nothing to compare.
        return {"producer_completed_once": len(producer_done) == 1,
                "three_attempts_in_order": three_attempts}, {}
    first, second, third = ids
    parent_done = producer_done[0]
    reference = {"task_id": producer_id, "completion_event_id": parent_done["id"]}
    resolved = {**reference, "summary": parent_done["payload"]["summary"], "result": expected}
    expected_child = {"handoff_marker": expected["handoff_marker"], "total": sum(expected["numbers"]),
                      "source_task_id": producer_id, "source_completion_event_id": parent_done["id"]}

    late_outcome_events = [e for e in by_assignment(OUTCOME_TOPICS, second)
                           if e["id"] != probe_event_id]
    frozen_returned = [r for r in records.get(frozen, [])
                       if r["event"] == "returned" and r["assignment_id"] == second]
    received = {name: [r["assignment"] for r in records.get(name, []) if r["event"] == "received"]
                for name in workers}
    accepted_id = state.tasks[consumer_id].completion_event_id
    accepted = next((e for e in events if e["id"] == accepted_id), None)

    checks = {
        "producer_completed_once": (parent_done["actor"] == "hermes"
                                    and parent_done["payload"].get("result") == expected),
        "three_attempts_in_order": assigned[0]["id"] < assigned[1]["id"] < assigned[2]["id"],
        "crashed_attempt_expired": (first in expired
                                    and expired[first]["id"] < assigned[1]["id"]
                                    and not by_assignment(OUTCOME_TOPICS, first)),
        "agent_process_killed_with_worker": agent_processes_killed >= 1,
        "frozen_attempt_expired": (second in expired
                                   and expired[second]["id"] < assigned[2]["id"]),
        "frozen_worker_returned_late": (len(frozen_returned) == 1
                                        and second in expired
                                        and frozen_returned[0]["t"] > expired[second]["ts"]),
        "late_result_not_accepted": all(e["id"] in rejected_ids for e in late_outcome_events),
        "stale_report_ignored_on_replay": (probe_event_id is not None
                                           and probe_event_id in rejected_ids),
        "final_result_from_last_attempt": (accepted is not None
                                           and accepted["topic"] == "task.completed"
                                           and accepted["actor"] == finisher
                                           and accepted["payload"]["assignment_id"] == third
                                           and accepted["payload"].get("result") == expected_child),
        "same_dependency_every_attempt": all(
            len(received[name]) == 1
            and received[name][0]["dependencies"] == [resolved]
            and received[name][0]["context"] == {}
            for name in workers),
        "assignments_use_completion_reference": all(
            e["payload"].get("dependency_refs") == [reference] for e in assigned),
        "both_tasks_completed_on_replay": all(
            state.tasks[i].status == "completed" for i in (producer_id, consumer_id)),
        "no_failures": not any(e["topic"] in {
            "task.attempt_failed", "task.failed", "task.dependency_failed",
        } and e["payload"].get("task_id") in (producer_id, consumer_id)
            and e["id"] not in rejected_ids for e in events),
    }
    facts = {
        "late_result_stopped_by": "reducer" if late_outcome_events else "worker runtime",
        "late_outcome_events_ignored": [{"id": e["id"], "topic": e["topic"]} for e in late_outcome_events],
        "frozen_worker_outcome": frozen_returned[0]["outcome"] if frozen_returned else None,
        "seconds_frozen_worker_returned_after_expiry": (
            round(frozen_returned[0]["t"] - expired[second]["ts"], 2)
            if frozen_returned and second in expired else None),
        "accepted_completion_event_id": accepted_id,
        "replay_ignored_events": len(rejected_ids),
    }
    return checks, facts


class Workers:
    """Spawn worker processes the way a user would, and interrupt them for real."""

    def __init__(self, harness: Harness, root: Path, heartbeat: float):
        self.harness = harness
        self.root = root
        self.environment = dict(harness.environment)
        existing = self.environment.get("PYTHONPATH")
        self.environment["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + existing if existing else "")
        self.environment["LOGPLANE_HEARTBEAT_SECONDS"] = str(heartbeat)
        self.frozen: set[str] = set()

    def start(self, name: str, argv: list[str]) -> None:
        mark = self.harness.last_event_id()
        log = (self.harness.logs / f"{name}.log").open("a", encoding="utf-8")
        process = subprocess.Popen([sys.executable, *argv], cwd=self.root, env=self.environment,
                                   stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        log.close()
        self.harness.processes[name] = process
        self.harness.wait_until(
            lambda: self.harness.find_event("agent.registered", actor=name, after=mark),
            30, f"worker {name} to register")

    def crash(self, name: str) -> int:
        """SIGKILL the worker and the agent processes it started, like a machine crash."""
        process = self.harness.processes.pop(name)
        found = subprocess.run(["pgrep", "-P", str(process.pid)], capture_output=True, text=True)
        children = [int(pid) for pid in found.stdout.split()]
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
        for child in children:
            # Each agent CLI leads its own process group; take its children too.
            for kill in (lambda: os.killpg(child, signal.SIGKILL), lambda: os.kill(child, signal.SIGKILL)):
                try:
                    kill()
                except (ProcessLookupError, PermissionError):
                    pass
        return len(children)

    def freeze(self, name: str) -> None:
        os.kill(self.harness.processes[name].pid, signal.SIGSTOP)
        self.frozen.add(name)

    def resume(self, name: str) -> None:
        os.kill(self.harness.processes[name].pid, signal.SIGCONT)
        self.frozen.discard(name)

    def resume_all(self) -> None:
        for name in list(self.frozen):
            try:
                self.resume(name)
            except (KeyError, ProcessLookupError):
                self.frozen.discard(name)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true",
                        help="approve one Hermes and up to three OpenClaw paid calls")
    parser.add_argument("--hermes-command", default="hermes")
    parser.add_argument("--hermes-model", default="openai/gpt-6-luna")
    parser.add_argument("--hermes-provider", default="nous")
    parser.add_argument("--openclaw-command", default="openclaw")
    parser.add_argument("--openclaw-model", default="nousportal/openai/gpt-6-luna")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("nous-proxy.json"))
    parser.add_argument("--lease-seconds", type=float, default=10.0,
                        help="how long a silent worker keeps its assignment (default 10)")
    parser.add_argument("--hold-seconds", type=float, default=2.0,
                        help="how long after a task starts to interrupt it (default 2)")
    parser.add_argument("--timeout", type=float, default=180.0, help="per agent call")
    parser.add_argument("--output", type=Path, help="new directory for retained trial evidence")
    args = parser.parse_args(argv)
    if not args.yes:
        parser.error("pass --yes to approve one Hermes and up to three OpenClaw paid calls")
    if args.lease_seconds <= 0 or args.hold_seconds < 0:
        parser.error("--lease-seconds must be positive and --hold-seconds non-negative")
    if args.output:
        root = args.output.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=False)
    else:
        root = Path(tempfile.mkdtemp(prefix="logplane-interruption-trial-"))
    workers = ("openclaw-a", "openclaw-b", "openclaw-c")
    for name in ("hermes", *workers, "records"):
        (root / name).mkdir()
    records_dir = root / "records"
    expected = {"handoff_marker": uuid.uuid4().hex, "numbers": [17, 25, 9]}

    harness = Harness(root, out=sys.stdout, lease_seconds=args.lease_seconds)
    heartbeat = harness.heartbeat_seconds
    procs = Workers(harness, root, heartbeat)
    config = args.config.expanduser().resolve()

    def openclaw(name):
        return ["-m", "examples.openclaw.run_worker", "--name", name, "--bus-url", harness.url,
                "--openclaw-command", args.openclaw_command, "--config", str(config),
                "--working-directory", str(root / name), "--model", args.openclaw_model,
                "--thinking", "high", "--timeout", str(args.timeout),
                "--heartbeat-seconds", str(heartbeat), "--record", str(records_dir / f"{name}.jsonl")]

    hermes = ["-m", "examples.hermes.run_worker", "--name", "hermes", "--bus-url", harness.url,
              "--working-directory", str(root / "hermes"), "--model", args.hermes_model,
              "--provider", args.hermes_provider, "--reasoning", "high",
              "--hermes-command", args.hermes_command, "--timeout", str(args.timeout),
              "--capability", "hermes"]

    evidence: dict = {"passed": False, "expected_producer_result": expected,
                      "lease_seconds": args.lease_seconds, "heartbeat_seconds": heartbeat}
    events: list[dict] = []
    step_timeout = args.timeout + 4 * args.lease_seconds + 30

    def step(text):
        evidence.setdefault("steps", []).append({"t": time.time(), "step": text})
        harness.say(text)

    try:
        harness.start_bus()
        harness.start_pm()
        producer, correlation = harness.submit(PRODUCER_GOAL, context=expected, capability="hermes",
                                               max_retries=0, quiet=True)
        consumer, _ = harness.submit(CONSUMER_GOAL, depends_on=[producer], context={},
                                     capability="openclaw", max_retries=2,
                                     correlation_id=correlation, quiet=True)
        evidence.update(producer_task_id=producer, consumer_task_id=consumer, correlation_id=correlation)

        procs.start("openclaw-a", openclaw("openclaw-a"))
        procs.start("hermes", hermes)
        step(f"Hermes runs task {producer}; OpenClaw worker A waits for it, then takes task {consumer}.")
        started = harness.wait_until(
            lambda: harness.find_event("task.started", task_id=consumer, actor="openclaw-a"),
            2 * step_timeout, "worker A to start the OpenClaw task")
        time.sleep(args.hold_seconds)
        killed = procs.crash("openclaw-a")
        evidence["agent_processes_killed"] = killed
        step(f"Killed worker A mid-task (kill -9), with the {killed} agent process(es) it started. "
             f"Nothing tells Logplane; A's lease just stops being renewed.")

        harness.wait_until(lambda: harness.find_event("task.assignment_expired", task_id=consumer,
                                                       after=started["id"]),
                           step_timeout, "worker A's attempt to expire")
        procs.start("openclaw-b", openclaw("openclaw-b"))
        step("Attempt 1 expired. Worker B starts and gets attempt 2.")
        started_b = harness.wait_until(
            lambda: harness.find_event("task.started", task_id=consumer, actor="openclaw-b"),
            step_timeout, "worker B to start attempt 2")
        time.sleep(args.hold_seconds)
        procs.freeze("openclaw-b")
        step("Froze worker B mid-task (SIGSTOP). Its agent keeps running, but B can't report "
             "or send heartbeats.")

        harness.wait_until(lambda: harness.find_event("task.assignment_expired", task_id=consumer,
                                                       after=started_b["id"]),
                           step_timeout, "worker B's attempt to expire")
        procs.start("openclaw-c", openclaw("openclaw-c"))
        step("Attempt 2 expired too. Worker C starts and gets attempt 3.")
        harness.wait_until(lambda: harness.task_state(consumer)["status"] in {
            "completed", "failed", "blocked", "dependency_failed", "cancelled"},
            step_timeout, "the OpenClaw task to finish")

        procs.resume("openclaw-b")
        step("Worker C finished. Resumed worker B, which now holds a late result for attempt 2.")
        frozen_record = records_dir / "openclaw-b.jsonl"
        harness.wait_until(lambda: any(r["event"] == "returned" for r in read_records(frozen_record)),
                           args.timeout + 30, "worker B's adapter to return")
        time.sleep(max(3.0, 2 * heartbeat))  # let any late publish reach the log

        # Probe: report attempt 2's result as worker B, the way a buggy or cut-off
        # worker might. Same idempotency key the runtime uses, so it cannot
        # duplicate a real late publish.
        assigned_b = next(e for e in harness.events(["task.assigned"])
                          if e["payload"].get("task_id") == consumer and e["actor"] == "pm"
                          and e["payload"].get("assignee") == "openclaw-b")
        b_assignment = assigned_b["payload"]["assignment_id"]
        b_result = next((r["outcome"].get("result") for r in read_records(frozen_record)
                         if r["event"] == "returned" and r["outcome"].get("result")), None)
        probe = BusClient(harness.url, actor="openclaw-b").publish("task.completed", {
            "task_id": consumer, "assignment_id": b_assignment,
            "worker_instance_id": assigned_b["payload"]["worker_instance_id"],
            "summary": "Late report for an attempt this worker no longer owns (trial probe).",
            "result": b_result or {"probe": True},
        }, caused_by=assigned_b["id"], idempotency_key=f"completed:{b_assignment}",
            correlation_id=correlation)
        evidence["stale_probe_event_id"] = probe["id"]
        step(f"Probe: sent attempt 2's completion as worker B (event #{probe['id']}). The log "
             "keeps it; the coordinator must ignore it.")

        events = harness.events()
        records = {name: read_records(records_dir / f"{name}.jsonl") for name in workers}
        checks, facts = verify_interruption(events, producer, consumer, expected, records,
                                            workers, killed, probe["id"])
        evidence.update(checks=checks, facts=facts, passed=all(checks.values()))
        for name, view in (("task", ["task", str(consumer)]), ("doctor", ["doctor", "--lease-seconds", str(args.lease_seconds)])):
            code, output = harness.view(view, show=False)
            (root / f"view-{name}.txt").write_text(output)
        print(json.dumps({"passed": evidence["passed"], "checks": checks, "facts": facts}, indent=2))
        if not evidence["passed"]:
            raise RuntimeError("interruption verification failed; inspect retained evidence")
    except Exception as exc:
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        procs.resume_all()
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
