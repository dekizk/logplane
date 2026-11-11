"""The contest hunt pipeline, end to end, with stand-in claude, codex and hermes."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from examples.contest_hunt.briefs import number_candidates
from examples.contest_hunt.run import production_diff, find_test_module
from examples.contest_hunt.worker import BriefingExecutor
from logplane.executors import AssignmentContext, Completed, PermanentFailure

REPO = Path(__file__).resolve().parents[1]

# Each stand-in reads the brief, finds the stage, and answers in that stage's shape.
STAGE_RESULTS = r'''
import json, re
def stage_result(text, model):
    stage = re.search(r"STAGE: (\w+)", text).group(1)
    if stage == "map":
        return {"areas": [
            {"id": "A01", "title": "Core maths", "files": ["src/lib.rs"], "focus": "rounding",
             "critical_paths": "drain via rounding"},
            {"id": "A02", "title": "Not in scope", "files": ["elsewhere.rs"], "focus": "x",
             "critical_paths": "x"}],
            "invariants": ["withdrawals never exceed deposits"], "notes": None}
    if stage == "hunt":
        area = re.search(r"AREA (A\d+)", text).group(1)
        if model == "quiet":
            return {"candidates": [], "checked": ["rounding is floor"], "notes": None}
        return {"candidates": [{"title": f"{model} finds a drain in {area}", "location": "src/lib.rs:3",
                 "root_cause": "rounds up", "attack_path": "1. deposit 2. withdraw", "preconditions": "none",
                 "impact": "drain", "funds_at_risk": "100%", "why_new": "not listed",
                 "confidence": "medium", "poc_outline": "cargo test"}], "checked": [], "notes": None}
    candidates = json.loads(open("contest/candidates.json").read()) if model != "hermes" else []
    ids = [c["id"] for c in candidates] or ["C01"]
    return {"findings": [{"rank": 1, "title": "Rounding drain", "candidate_ids": ids[:1],
             "verdict": "needs_poc", "location": "src/lib.rs:3", "attack_path": "deposit, withdraw",
             "why_new": "not known", "poc_plan": "write a test", "reviewer_notes": "check bounds"}],
            "rejected": [{"candidate_id": i, "reason": "duplicate of C01"} for i in ids[1:]],
            "summary": f"{len(ids)} candidates reviewed"}
'''

FAKE_CLAUDE = STAGE_RESULTS + r'''
import os, sys
argv = sys.argv[1:]
if argv == ["auth", "status"]:
    print(json.dumps({"loggedIn": True, "authMethod": "claude.ai"})); sys.exit(0)
assert argv[argv.index("--tools") + 1] == "Read,Grep,Glob", argv
text = sys.stdin.read()
result = stage_result(text, os.environ.get("FAKE_CLAUDE_PERSONA", "claude"))
print(json.dumps({"type": "system", "subtype": "init", "apiKeySource": "none",
                  "tools": ["Read", "Grep", "Glob", "StructuredOutput"], "model": "claude-test",
                  "mcp_servers": [], "permissionMode": "dontAsk", "claude_code_version": "9"}))
print(json.dumps({"type": "result", "subtype": "success", "is_error": False, "num_turns": 3,
                  "total_cost_usd": 0.5, "usage": {"input_tokens": 10, "output_tokens": 5,
                  "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0},
                  "structured_output": {"status": "completed", "summary": "done", "result": result}}))
'''

FAKE_CODEX = STAGE_RESULTS + r'''
import os, sys
argv = sys.argv[1:]
if argv[:2] == ["login", "status"]:
    sys.stderr.write("Logged in using ChatGPT\n"); sys.exit(0)
assert argv[argv.index("--sandbox") + 1] == "read-only", argv
os.chdir(argv[argv.index("--cd") + 1])
text = sys.stdin.read()
outcome = {"status": "completed", "summary": "done", "reason": None, "code": None,
           "result": stage_result(text, "codex")}
print(json.dumps({"type": "item.completed", "item": {"id": "1", "type": "agent_message",
                  "text": json.dumps(outcome)}}))
print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 100, "cached_input_tokens": 0,
                  "output_tokens": 10, "reasoning_output_tokens": 0}}))
'''

FAKE_HERMES = STAGE_RESULTS + r'''
import sys, pathlib
argv = sys.argv[1:]
prompt = argv[argv.index("-z") + 1]
assert "===== contest/rules.md =====" in prompt, "hermes must get the rules inline"
assert "src/lib.rs" in prompt
pathlib.Path(argv[argv.index("--usage-file") + 1]).write_text(json.dumps({"total_tokens": 7}))
print(json.dumps({"status": "completed", "summary": "done", "result": stage_result(prompt, "hermes")}))
'''


def git(cwd, *args):
    """Run git in ``cwd`` and return the current HEAD afterwards."""
    identity = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                   env={**os.environ, **identity})
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd, capture_output=True, text=True)
    return head.stdout.strip()


def make_contest(root: Path) -> tuple[Path, Path]:
    repo = root / "contest-repo"
    (repo / "src").mkdir(parents=True)
    git(repo, "init", "-q")
    (repo / "src" / "lib.rs").write_text("pub fn rate() -> i128 {\n    1\n}\n")
    git(repo, "add", ".")
    base = git(repo, "commit", "-qm", "base")
    (repo / "src" / "lib.rs").write_text(
        "pub fn rate() -> i128 {\n    2\n}\n\npub fn drain() {}\n\n#[cfg(test)]\nmod tests {\n"
        "    #[test]\n    fn t() { assert_eq!(super::rate(), 2); }\n}\n")
    git(repo, "add", ".")
    final = git(repo, "commit", "-qm", "final")
    contest = root / "contest"
    contest.mkdir()
    for name in ("rules", "known_issues", "primer"):
        (contest / f"{name}.md").write_text(f"# {name}\n")
    (contest / "contest.json").write_text(json.dumps({
        "name": "toy", "title": "Toy contest", "scope_commit": final, "checkout_commit": final,
        "diff_base": base, "diff_final": final, "scope_files": ["src/lib.rs"],
        "rules": "rules.md", "known_issues": "known_issues.md", "primer": "primer.md",
        "seed_areas": [{"id": "A01", "title": "Core", "files": ["src/lib.rs"], "focus": "all"}]}))
    return repo, contest / "contest.json"


def stand_in(root: Path, name: str, body: str) -> str:
    path = root / f"fake_{name}"
    path.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
    path.chmod(0o755)
    return str(path)


class ContestHuntTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_diff_keeps_production_code_and_drops_the_test_module(self):
        repo, contest = make_contest(self.root)
        config = json.loads(contest.read_text())
        diff = production_diff(repo, config["diff_base"], config["diff_final"], "src/lib.rs", "#[cfg(test)]")
        self.assertIn("+    2", diff)
        self.assertIn("+pub fn drain() {}", diff)
        self.assertNotIn("#[test]", diff)
        self.assertNotIn("mod tests", diff)
        self.assertIsNone(find_test_module("#[cfg(test)]\nuse x::y;\nfn a() {}\n", "#[cfg(test)]"))
        self.assertEqual(2, find_test_module("fn a() {}\n#[cfg(test)]\n#[allow(x)]\nmod t {}\n", "#[cfg(test)]"))

    def test_briefing_inlines_only_for_tool_less_runners_and_stays_in_the_workspace(self):
        workspace = self.root / "ws"
        (workspace / "contest").mkdir(parents=True)
        (workspace / "contest" / "brief.md").write_text("STAGE: hunt\nDo it")
        (workspace / "contest" / "rules.md").write_text("RULES TEXT")
        seen = []

        class Delegate:
            last_report = None

            def execute(self, assignment):
                seen.append(assignment)
                return Completed("ok", {"candidates": []})

        def run(inline, context, stage="hunt"):
            executor = BriefingExecutor(Delegate(), workspace=workspace, stage=stage, inline=inline)
            return executor.execute(AssignmentContext(
                correlation_id="c", task_id=1, assignment_id="task:1:attempt:1", assignment_event_id=2,
                attempt=1, goal="Hunt A01", assignee="w", worker_instance_id="i", context=context))

        context = {"brief": "contest/brief.md", "inline_files": ["contest/rules.md"], "area": "A01"}
        self.assertIsInstance(run(False, context), Completed)
        self.assertEqual("STAGE: hunt\nDo it", seen[-1].goal)
        self.assertEqual({"area": "A01"}, dict(seen[-1].context))
        run(True, context)
        self.assertIn("===== contest/rules.md =====\nRULES TEXT", seen[-1].goal)
        self.assertEqual("contest_brief_unreadable",
                         run(True, {"brief": "../outside.md"}).code)
        self.assertEqual("contest_no_brief", run(False, {}).code)
        bad = run(False, context, stage="triage")
        self.assertIsInstance(bad, PermanentFailure)
        self.assertEqual("contest_bad_result", bad.code)

    def test_numbering_keeps_provenance(self):
        hunts = [{"area": "A01", "model": "x", "result": {"candidates": [{"title": "a"}, {"title": "b"}]}},
                 {"area": "A02", "model": "y", "result": {"candidates": [{"title": "c"}]}}]
        numbered = number_candidates(hunts)
        self.assertEqual(["C01", "C02", "C03"], [c["id"] for c in numbered])
        self.assertEqual(("A02", "y"), (numbered[2]["area"], numbered[2]["model"]))

    def test_full_run_with_stand_in_agents(self):
        repo, contest = make_contest(self.root)
        claude = stand_in(self.root, "claude", FAKE_CLAUDE)
        codex = stand_in(self.root, "codex", FAKE_CODEX)
        hermes = stand_in(self.root, "hermes", FAKE_HERMES)
        config_dir = self.root / "claude-config"
        config_dir.mkdir()
        models = self.root / "models.json"
        models.write_text(json.dumps({
            "models": {
                "mapper": {"runner": "claude", "command": claude, "model": "fable"},
                "astra": {"runner": "codex", "command": codex, "model": "gpt-test", "effort": "high"},
                "opus": {"runner": "claude", "command": claude, "model": "opus"},
                "glm": {"runner": "hermes", "command": hermes, "provider": "nous", "model": "glm-test"},
            },
            "stages": {"map": "mapper", "hunt": ["astra", "opus", "glm"], "triage": "astra"}}))
        out = self.root / "run"
        done = subprocess.run(
            [sys.executable, "-m", "examples.contest_hunt.run", "--yes", "--contest", str(contest),
             "--models", str(models), "--repo", str(repo), "--output", str(out), "--offline",
             "--lease-seconds", "8", "--stage-timeout-hours", "0.05"],
            cwd=REPO, capture_output=True, text=True, timeout=240,
            env={**os.environ, "CLAUDE_CONFIG_DIR": str(config_dir)})
        self.assertEqual(0, done.returncode, done.stdout[-3000:] + done.stderr[-3000:])
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual("map", summary["areas_from"])
        self.assertEqual([{"id": "A01", "title": "Core maths"}], summary["areas"],
                         "areas naming files outside the scope are dropped")
        self.assertEqual({"completed": 3}, summary["hunt_counts"])
        candidates = json.loads((out / "results" / "candidates.json").read_text())
        self.assertEqual(3, len(candidates))
        self.assertEqual({"astra", "opus", "glm"}, {c["model"] for c in candidates})
        triage = json.loads((out / "results" / "triage.json").read_text())
        self.assertEqual(["C01"], triage["findings"][0]["candidate_ids"])
        self.assertEqual(2, len(triage["rejected"]))
        report = (out / "report.md").read_text()
        self.assertIn("Rounding drain (needs_poc)", report)
        self.assertIn("C03", report)
        self.assertIn("withdrawals never exceed deposits",
                      (out / "workspace" / "contest" / "briefs" / "hunt-A01.md").read_text())
        self.assertNotIn("#[test]", (out / "workspace" / "contest" / "scope.diff").read_text())
        self.assertTrue((out / "records" / "glm-hunt.jsonl").exists())
        self.assertGreater(summary["workflow"]["tokens"], 0)


if __name__ == "__main__":
    unittest.main()
