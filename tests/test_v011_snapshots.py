import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from logplane import bus
from logplane.cli import main
from logplane.local_config import initialize_local_config, LocalConfig
from logplane.pm_agent import initial_cursor, plan_next_emission
from logplane.projection import replay_events, PROJECTION_TOPICS, apply_event
from logplane.projection_store import (load_projection, save_projection, clear_projection,
                              encode_state, decode_state)
from tests.test_pm_agent import registered, created, assigned, event


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original = bus.DB_PATH
        bus.DB_PATH = Path(self.temp.name) / "events.db"
        bus.init_db()
        self.path = bus.DB_PATH

    def tearDown(self):
        bus.DB_PATH = self.original
        self.temp.cleanup()

    def seed(self):
        # Exercise typed containers, policy, accounting and an active DAG.
        records = [registered(), created(task_id=1, correlation_id="flow"),
                   created(3, task_id=2, depends_on=(1,), correlation_id="flow"),
                   assigned(4, attempt=1, task_id=1)]
        records[0]["payload"]["capabilities"] = ["python"]
        records[1]["payload"]["context"] = {"dict": [["record", "ordinary user data"]]}
        records += [event(5, "workflow.policy_set", "pm", {
            "source": "default", "reason": "fixture", "max_active_assignments": 1,
            "max_total_tokens": 10000, "reserve_tokens_per_assignment": 1000,
        }, correlation_id="flow"), event(6, "workflow.pause_requested", "human", {
            "reason": "inspect"}, correlation_id="flow")]
        with bus.db() as conn:
            for ev in records:
                conn.execute("INSERT INTO events(id,ts,topic,actor,schema_version,correlation_id,payload) "
                             "VALUES (?,?,?,?,?,?,?)", (ev["id"], ev["ts"], ev["topic"], ev["actor"],
                                                        2, ev["correlation_id"], json.dumps(ev["payload"])))
        bus.rebuild_task_index()

    def corrupt(self, transform):
        with bus.db() as conn:
            row = conn.execute("SELECT envelope FROM coordination_snapshot").fetchone()
            value = json.loads(row[0])
            transform(value)
            data = json.dumps(value)
            conn.execute("UPDATE coordination_snapshot SET envelope=?,sha256=?",
                         (data, hashlib.sha256(data.encode()).hexdigest()))

    def assert_equivalent(self, result):
        events = bus.fetch_after(0, list(PROJECTION_TOPICS))
        expected = replay_events(events)
        self.assertEqual(vars(expected), vars(result.state))
        self.assertEqual(plan_next_emission(expected, now=101),
                         plan_next_emission(result.state, now=101))

    def test_codec_preserves_complete_state_and_next_decision(self):
        self.seed()
        result = load_projection(self.path)
        self.assert_equivalent(result)
        self.assertEqual(vars(result.state), vars(decode_state(encode_state(result.state))))
        save_projection(self.path, result)
        warmed = load_projection(self.path)
        self.assertEqual(6, warmed.snapshot_event_id)
        self.assertEqual(0, warmed.replayed_events)
        self.assert_equivalent(warmed)
        # Apply the same later control to both restored and independently replayed states.
        later = event(7, "task.cancel_requested", "human", {"task_id": 1, "reason": "stop"})
        expected = replay_events(bus.fetch_after(0, list(PROJECTION_TOPICS)))
        apply_event(expected, later)
        apply_event(warmed.state, later)
        self.assertEqual(vars(expected), vars(warmed.state))
        self.assertEqual(plan_next_emission(expected, now=102), plan_next_emission(warmed.state, now=102))

    def test_codec_preserves_usage_tuples_decisions_and_agent_policies(self):
        from logplane.projection import AgentPolicyRecord
        self.seed()
        state = load_projection(self.path).state
        self.assertIn("task:1:attempt:1", state.assignment_accounting)
        state.assignment_accounting["task:1:attempt:1"].usage["invocation-1"] = (9, 42, None)
        state.agent_policies["alice"] = AgentPolicyRecord("alice", 10, "human", "operator", "cap", 2)
        state.tasks[1].decisions = [{"values": {"target": "staging"}}]
        state.last_scheduled_workflow_key = (2, "flow")
        decoded = decode_state(encode_state(state))
        self.assertEqual(vars(state), vars(decoded))
        self.assertEqual(state.assignment_accounting["task:1:attempt:1"].charged_tokens(),
                         decoded.assignment_accounting["task:1:attempt:1"].charged_tokens())

    def test_decode_rejects_non_finite_numbers(self):
        self.seed()
        encoded = encode_state(load_projection(self.path).state)
        for constant in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(constant=constant):
                corrupt = encoded.replace('["last_scheduling_assignment_event_id",4]',
                                          f'["last_scheduling_assignment_event_id",{constant}]', 1)
                self.assertNotEqual(encoded, corrupt)
                with self.assertRaisesRegex(ValueError, "invalid snapshot constant"):
                    decode_state(corrupt)

    def test_fingerprint_changes_when_a_reducer_default_changes(self):
        from logplane import projection, projection_store

        function = projection.AgentPolicyRecord.__init__
        fingerprint = projection_store.reducer_fingerprint.__wrapped__  # bypass the cache
        before = fingerprint()
        original = function.__defaults__
        try:
            function.__defaults__ = (5,)
            self.assertNotEqual(before, fingerprint())
        finally:
            function.__defaults__ = original
        self.assertEqual(before, fingerprint())

    def test_fingerprint_changes_when_a_topic_is_routed_to_another_handler(self):
        from logplane import projection, projection_store

        fingerprint = projection_store.reducer_fingerprint.__wrapped__  # bypass the cache
        before = fingerprint()
        original = projection._TASK_HANDLERS["task.paused"]
        try:
            projection._TASK_HANDLERS["task.paused"] = projection._TASK_HANDLERS["task.resumed"]
            self.assertNotEqual(before, fingerprint())
        finally:
            projection._TASK_HANDLERS["task.paused"] = original
        self.assertEqual(before, fingerprint())

    def test_fingerprint_is_stable_across_processes(self):
        script = "from logplane.projection_store import reducer_fingerprint; print(reducer_fingerprint())"
        values = {subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                                 check=True, cwd=Path(bus.__file__).parents[1]).stdout.strip()
                  for _ in range(2)}
        self.assertEqual(1, len(values))

    def test_suffix_replay_and_historical_bound_never_use_future_snapshot(self):
        self.seed()
        save_projection(self.path, load_projection(self.path))
        bus.append_event("custom.telemetry", "test", {"large": "ignored"})
        bus.append_event("task.created", "human", {"title": "next"})
        result = load_projection(self.path)
        self.assertEqual(1, result.replayed_events)
        self.assertEqual(8, result.last_event_id)
        self.assert_equivalent(result)
        save_projection(self.path, result)
        prefix = load_projection(self.path, through_id=3)
        self.assertIsNone(prefix.snapshot_event_id)
        expected = replay_events(bus.fetch_after(0, list(PROJECTION_TOPICS))[:3])
        self.assertEqual(vars(expected), vars(prefix.state))

    def test_incompatible_snapshots_fall_back_to_full_replay(self):
        self.seed()
        changes = [lambda x: x.update(format=999), lambda x: x.update(reducer="old"),
                   lambda x: x.update(database_id="another"), lambda x: x.update(head=999),
                   lambda x: x.update(anchor="wrong"), lambda x: x.update(state='{"evil":"code"}')]
        for change in changes:
            save_projection(self.path, load_projection(self.path, use_snapshot=False))
            self.corrupt(change)
            result = load_projection(self.path)
            self.assertIsNone(result.snapshot_event_id)
            self.assertEqual(6, result.replayed_events)
            self.assert_equivalent(result)

    def test_checksum_damage_and_delete_are_disposable(self):
        self.seed()
        events = bus.fetch_after(0, None)
        save_projection(self.path, load_projection(self.path))
        with bus.db() as conn:
            conn.execute("UPDATE coordination_snapshot SET sha256='broken'")
        result = load_projection(self.path)
        self.assertIn("checksum", result.cache_note)
        self.assert_equivalent(result)
        clear_projection(self.path)
        self.assert_equivalent(load_projection(self.path))
        self.assertEqual(events, bus.fetch_after(0, None))

    def test_replay_is_pinned_when_a_publisher_appends_during_read(self):
        self.seed()
        original = apply_event
        appended = False
        def during_replay(state, ev):
            nonlocal appended
            if not appended:
                appended = True
                bus.append_event("task.created", "human", {"title": "racing publish"})
            return original(state, ev)
        with patch("logplane.projection_store.projection.apply_event", side_effect=during_replay):
            result = load_projection(self.path, use_snapshot=False)
        self.assertEqual(6, result.last_event_id)
        self.assertEqual(2, len(result.state.tasks))
        save_projection(self.path, result)
        recovered = load_projection(self.path)
        self.assertEqual(1, recovered.replayed_events)
        self.assert_equivalent(recovered)

    def test_http_identity_and_anchor_round_trip(self):
        from logplane.client import BusClient
        from fastapi.testclient import TestClient
        self.seed()
        with patch.dict(os.environ, {}, clear=False), TestClient(bus.app) as server:
            remote = BusClient("http://testserver", "pm", offset_dir=Path(self.temp.name) / "offsets")
            def get(url, **kwargs):
                kwargs.pop("timeout", None)  # TestClient has no real network timeout.
                return server.get(url, **kwargs)
            with patch("logplane.client.httpx.get", side_effect=get):
                cursor = initial_cursor(remote, self.path)
        self.assertEqual(6, cursor.last_event_id)
        self.assertEqual(vars(load_projection(self.path).state), vars(cursor.state))

    def test_snapshot_write_failure_and_process_loss_preserve_old_cache(self):
        self.seed()
        save_projection(self.path, load_projection(self.path))
        with bus.db() as conn:
            before = tuple(conn.execute("SELECT * FROM coordination_snapshot").fetchone())
            conn.execute("CREATE TRIGGER fail_snapshot BEFORE INSERT ON coordination_snapshot "
                         "BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            save_projection(self.path, load_projection(self.path))
        with bus.db() as conn:
            self.assertEqual(before, tuple(conn.execute("SELECT * FROM coordination_snapshot").fetchone()))
            conn.execute("DROP TRIGGER fail_snapshot")
        child = subprocess.run([sys.executable, "-c",
            "import os,sqlite3,sys; c=sqlite3.connect(sys.argv[1]); "
            "c.execute('BEGIN IMMEDIATE'); c.execute('DELETE FROM coordination_snapshot'); os._exit(77)",
            str(self.path)], capture_output=True, timeout=10)
        self.assertEqual(77, child.returncode)
        self.assertEqual(6, load_projection(self.path).snapshot_event_id)

    def test_snapshot_is_compatible_in_a_fresh_python_process(self):
        self.seed()
        save_projection(self.path, load_projection(self.path))
        child = subprocess.run([sys.executable, "-c",
            "from pathlib import Path; import sys; from logplane.projection_store import load_projection; "
            "r=load_projection(Path(sys.argv[1])); print(r.cache_note); "
            "assert r.snapshot_event_id==6, r.cache_note", str(self.path)],
            cwd=Path(bus.__file__).parents[1], capture_output=True, timeout=10)
        self.assertEqual(0, child.returncode, child.stdout + child.stderr)

    def test_pm_checks_bus_identity_and_survives_cache_write_failure(self):
        self.seed()
        result = load_projection(self.path)
        class Remote:
            def database_identity(self):
                return result.database_id
            def get_event(self, event_id):
                return bus.fetch_event(event_id)
        with patch("logplane.projection_store.save_projection", side_effect=sqlite3.OperationalError("locked")):
            cursor = initial_cursor(Remote(), self.path)
        self.assertEqual(6, cursor.last_event_id)
        self.assert_equivalent(result)
        with patch.object(Remote, "database_identity", return_value="different"):
            with self.assertRaisesRegex(ValueError, "does not match"):
                initial_cursor(Remote(), self.path)
        with patch.object(Remote, "get_event", return_value={"id": 6, "payload": {"other": True}}):
            with self.assertRaisesRegex(ValueError, "anchor does not match"):
                initial_cursor(Remote(), self.path)

    def test_database_identity_persists_and_cli_build_clear(self):
        config = initialize_local_config(Path(self.temp.name) / "local")
        local = LocalConfig.from_file(config)
        local.database_path.parent.mkdir(parents=True, exist_ok=True)
        bus.DB_PATH = local.database_path
        bus.init_db()
        identity = load_projection(bus.DB_PATH).database_id
        bus.init_db()
        self.assertEqual(identity, load_projection(bus.DB_PATH).database_id)
        with patch.dict(os.environ):
            for action in ("build", "clear"):
                output = io.StringIO()
                self.assertEqual(0, main(["snapshot", action, "--config", str(config), "--json"], stdout=output))
                self.assertTrue(json.loads(output.getvalue())["ok"])
        self.assertIsNone(load_projection(bus.DB_PATH).snapshot_event_id)


if __name__ == "__main__":
    unittest.main()
