"""The interruption trial, run end to end with stand-in agents (no model calls)."""

import copy
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from examples.openclaw.interruption_trial import read_records, verify_interruption

REPO = Path(__file__).resolve().parents[1]
WORKERS = ("openclaw-a", "openclaw-b", "openclaw-c")

FAKE_OPENCLAW = '''
import json, os, pathlib, sys, time
args = sys.argv[1:]
assert args[:2] == ["agent", "exec"], args
prompt = pathlib.Path(args[args.index("--message-file") + 1]).read_text()
assignment = json.loads(prompt.split("Assignment:\\n", 1)[1])
time.sleep(2)
dep = assignment["dependencies"][0]
result = {"handoff_marker": dep["result"]["handoff_marker"], "total": sum(dep["result"]["numbers"]),
          "source_task_id": dep["task_id"], "source_completion_event_id": dep["completion_event_id"]}
final = {"status": "completed", "summary": "Combined the upstream result.", "result": result}
print(json.dumps({"ok": True, "status": "ok", "final": json.dumps(final), "usage": {"total": 7}}))
'''

FAKE_HERMES = '''
import json, pathlib, sys
args = sys.argv[1:]
assignment = json.loads(args[args.index("-z") + 1].split("ASSIGNMENT_JSON\\n", 1)[1])
pathlib.Path(args[args.index("--usage-file") + 1]).write_text(json.dumps({"total_tokens": 5}))
print(json.dumps({"status": "completed", "summary": "Returned the context unchanged.",
                  "result": assignment["context"]}))
'''


@unittest.skipUnless(shutil.which("pgrep"), "the trial finds agent processes with pgrep")
class InterruptionTrialTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        for name, body in (("openclaw", FAKE_OPENCLAW), ("hermes", FAKE_HERMES)):
            path = root / f"fake_{name}"
            path.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
            path.chmod(0o755)
        config = root / "config.json"
        config.write_text('{"tools": {"deny": ["*"]}, "plugins": {"enabled": false}}')
        cls.evidence = root / "evidence"
        cls.process = subprocess.run(
            [sys.executable, "-m", "examples.openclaw.interruption_trial", "--yes",
             "--hermes-command", str(root / "fake_hermes"),
             "--openclaw-command", str(root / "fake_openclaw"), "--config", str(config),
             "--lease-seconds", "4", "--hold-seconds", "0.5", "--timeout", "30",
             "--output", str(cls.evidence)],
            cwd=REPO, capture_output=True, text=True, timeout=150,
        )
        cls.summary = json.loads((cls.evidence / "summary.json").read_text())
        cls.events = json.loads((cls.evidence / "events.json").read_text())
        cls.records = {name: read_records(cls.evidence / "records" / f"{name}.jsonl")
                       for name in WORKERS}

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def verify(self, events=None, records=None, killed=1, probe="default"):
        summary = self.summary
        return verify_interruption(
            events if events is not None else self.events,
            summary["producer_task_id"], summary["consumer_task_id"],
            summary["expected_producer_result"],
            records if records is not None else self.records, WORKERS, killed,
            summary["stale_probe_event_id"] if probe == "default" else probe,
        )[0]

    def test_stand_in_run_passes_every_check(self):
        self.assertEqual(0, self.process.returncode, self.process.stdout[-3000:] + self.process.stderr[-3000:])
        self.assertTrue(self.summary["passed"], self.summary.get("checks"))
        self.assertTrue(all(self.verify().values()))
        self.assertEqual(1, self.summary["facts"]["replay_ignored_events"])
        finished = next(r for r in self.records["openclaw-c"] if r["event"] == "returned")
        self.assertEqual({"total": 7}, finished["openclaw_report"]["usage"])
        doctor = (self.evidence / "view-doctor.txt").read_text()
        self.assertIn("1 historical event(s) were safely ignored", doctor)

    def test_missing_expiry_is_caught(self):
        crashed = next(e for e in self.events if e["topic"] == "task.assignment_expired")
        events = [e for e in self.events if e is not crashed]
        checks = self.verify(events=events)
        self.assertFalse(all(checks.values()), checks)
        self.assertFalse(checks.get("crashed_attempt_expired", False))

    def test_wrong_final_result_is_caught(self):
        events = copy.deepcopy(self.events)
        final = next(e for e in events if e["id"] == self.summary["facts"]["accepted_completion_event_id"])
        final["payload"]["result"]["total"] += 1
        self.assertFalse(self.verify(events=events)["final_result_from_last_attempt"])

    def test_frozen_worker_must_return_after_its_expiry(self):
        records = copy.deepcopy(self.records)
        for record in records["openclaw-b"]:
            record["t"] = 0.0
        self.assertFalse(self.verify(records=records)["frozen_worker_returned_late"])

    def test_every_attempt_must_get_the_same_dependency(self):
        records = copy.deepcopy(self.records)
        received = next(r for r in records["openclaw-c"] if r["event"] == "received")
        received["assignment"]["dependencies"][0]["result"]["numbers"] = [1]
        self.assertFalse(self.verify(records=records)["same_dependency_every_attempt"])

    def test_crash_must_take_the_agent_process_with_it(self):
        self.assertFalse(self.verify(killed=0)["agent_process_killed_with_worker"])

    def test_probe_is_required_and_must_be_ignored(self):
        self.assertFalse(self.verify(probe=None)["stale_report_ignored_on_replay"])
        accepted = self.summary["facts"]["accepted_completion_event_id"]
        self.assertFalse(self.verify(probe=accepted)["stale_report_ignored_on_replay"])


if __name__ == "__main__":
    unittest.main()
