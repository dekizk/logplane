"""Run a contest hunt through Logplane: map, hunt with several models, triage.

  python -m examples.contest_hunt.run --yes \\
      --contest examples/contest_hunt/contests/precept/contest.json \\
      --models my-models.json --repo ~/audits/precept

Each stage is a Logplane task served by a worker process per model. The run
keeps its own bus and coordinator in the output directory, and writes
report.md there at the end. Nothing is ever submitted to the contest.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from examples.contest_hunt import briefs
from logplane.demo import Harness

REPO_ROOT = Path(__file__).resolve().parents[2]
TERMINAL = {"completed", "failed", "cancelled", "dependency_failed", "superseded", "blocked"}
RUNNER_ONLY_INLINE = "hermes"


# ---------------------------------------------------------------- workspace
def git(repo: Path, *args: str, binary: bool = False):
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, check=False)
    if done.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} failed: {done.stderr.decode(errors='replace').strip()}")
    return done.stdout if binary else done.stdout.decode("utf-8", errors="replace")


def production_diff(repo: Path, base: str, final: str, path: str, marker: str) -> str:
    """The diff of one file, without hunks inside its test module."""
    try:
        text = git(repo, "show", f"{final}:{path}")
    except SystemExit:
        text = ""
    test_start = find_test_module(text, marker)
    header, hunks, keep, seen_hunk, new_line = [], [], False, False, 0
    for line in git(repo, "diff", "--no-color", "-U3", base, final, "--", path).splitlines():
        if line.startswith("@@"):
            seen_hunk = True
            match = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)", line)
            new_line = int(match.group(1)) if match else 0
            keep = match is not None and (test_start is None or new_line < test_start)
            if keep:
                hunks.append(line)
        elif not seen_hunk:
            header.append(line)  # diff --git, index, ---/+++ lines
        elif keep:
            if test_start is not None and new_line >= test_start:
                keep = False  # this hunk runs on into the test module: cut it here
                continue
            hunks.append(line)
            if not line.startswith("-"):
                new_line += 1
    return "\n".join(header + hunks) + "\n" if hunks else ""


def find_test_module(text: str, marker: str) -> int | None:
    """Line number of the ``#[cfg(test)]`` that opens a test module, if any.

    A ``#[cfg(test)]`` on a single helper or import is not the module, so the
    marker must be followed (after blank lines and attributes) by ``mod``.
    """
    lines = text.splitlines()
    for number, line in enumerate(lines, 1):
        if not line.strip().startswith(marker):
            continue
        for following in lines[number:]:
            stripped = following.strip()
            if not stripped or stripped.startswith("#["):
                continue
            if re.match(r"(pub(\([^)]*\))?\s+)?mod\s+\w+", stripped):
                return number
            break
    return None


def prepare_workspace(config: dict, contest_dir: Path, repo: Path, out: Path, offline: bool) -> Path:
    for commit in ("scope_commit", "checkout_commit", "diff_base", "diff_final"):
        git(repo, "cat-file", "-e", f"{config[commit]}^{{commit}}")
    same = subprocess.run(["git", "-C", str(repo), "diff", "--quiet", config["scope_commit"],
                           config["checkout_commit"], "--", *config["scope_files"]])
    if same.returncode != 0:
        raise SystemExit("in-scope code differs between the scope commit and the checkout commit")
    workspace = out / "workspace"
    (workspace / "repo").mkdir(parents=True)
    archive = git(repo, "archive", "--format=tar", config["checkout_commit"], binary=True)
    subprocess.run(["tar", "-x", "-C", str(workspace / "repo")], input=archive, check=True)
    contest = workspace / "contest"
    (contest / "diffs").mkdir(parents=True)
    (contest / "briefs").mkdir()
    (contest / "docs").mkdir()
    for key in ("rules", "known_issues", "primer"):
        shutil.copyfile(contest_dir / config[key], contest / f"{key}.md")
    (contest / "scope_files.txt").write_text("\n".join(config["scope_files"]) + "\n")
    marker = config.get("test_module_marker", "#[cfg(test)]")
    per_file = {path: production_diff(repo, config["diff_base"], config["diff_final"], path, marker)
                for path in config["scope_files"]}
    (contest / "scope.diff").write_text("".join(per_file[p] for p in config["scope_files"]))
    (contest / "per_file_diffs.json").write_text(json.dumps(per_file))
    for doc in config.get("extra_docs", []):
        if offline:
            break
        target = contest / "docs" / Path(doc["path"]).name
        try:
            with urllib.request.urlopen(doc["url"], timeout=30) as response:
                target.write_bytes(response.read())
        except OSError as exc:
            print(f"warning: could not fetch {doc['url']}: {exc}", file=sys.stderr)
    return workspace


def write_area_diff(workspace: Path, area: dict) -> str:
    per_file = json.loads((workspace / "contest" / "per_file_diffs.json").read_text())
    relative = f"contest/diffs/{area['id']}.diff"
    (workspace / relative).write_text("".join(per_file.get(path, "") for path in area["files"]))
    return relative


# ---------------------------------------------------------------- workers
class Workers:
    def __init__(self, harness: Harness, out: Path, workspace: Path, heartbeat: float):
        self.harness, self.out, self.workspace, self.heartbeat = harness, out, workspace, heartbeat
        self.environment = dict(harness.environment)
        existing = self.environment.get("PYTHONPATH")
        self.environment["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + existing if existing else "")
        (out / "records").mkdir(exist_ok=True)

    def start(self, name: str, stage: str, spec: dict) -> None:
        argv = ["-m", "examples.contest_hunt.worker", "--runner", spec["runner"], "--stage", stage,
                "--name", name, "--workspace", str(self.workspace), "--bus-url", self.harness.url,
                "--heartbeat-seconds", str(self.heartbeat), "--capacity", str(spec.get("capacity", 1)),
                "--record", str(self.out / "records" / f"{name}.jsonl")]
        for key in ("model", "effort", "provider", "command", "timeout", "max_turns", "max_budget_usd"):
            if spec.get(key) is not None:
                argv += ["--" + key.replace("_", "-"), str(spec[key])]
        if spec.get("daybreak") is True:
            argv.append("--daybreak")
        mark = self.harness.last_event_id()
        log = (self.harness.logs / f"{name}.log").open("a", encoding="utf-8")
        process = subprocess.Popen([sys.executable, *argv], cwd=self.out, env=self.environment,
                                   stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        log.close()
        self.harness.processes[name] = process

        def registered():
            if process.poll() is not None:
                raise SystemExit(f"worker {name} exited during startup; see {self.harness.logs / (name + '.log')}")
            return self.harness.find_event("agent.registered", actor=name, after=mark)
        deadline = time.monotonic() + 90
        while not registered():
            if time.monotonic() > deadline:
                raise SystemExit(f"worker {name} did not register; see {self.harness.logs}")
            time.sleep(0.5)


# ---------------------------------------------------------------- tasks
def wait_tasks(harness: Harness, task_ids: list[int], timeout: float, label: str) -> dict[int, dict]:
    deadline, last = time.monotonic() + timeout, None
    while True:
        states = {task_id: harness.task_state(task_id) for task_id in task_ids}
        done = sum(state.get("status") in TERMINAL for state in states.values())
        if done != last:
            print(f"[contest] {label}: {done}/{len(task_ids)} finished", flush=True)
            last = done
        if done == len(task_ids):
            return states
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out waiting for {label}; the run's history is kept in {harness.workdir}")
        time.sleep(5)


def completed_results(harness: Harness) -> dict[int, dict]:
    return {event["payload"]["task_id"]: event["payload"].get("result") or {}
            for event in harness.events(["task.completed"])}


def choose_areas(config: dict, mapped: dict | None) -> tuple[list[dict], list[str], str]:
    scope = set(config["scope_files"])
    if mapped:
        areas = []
        for area in mapped.get("areas", []):
            files = [f for f in area.get("files", []) if f in scope]
            if files and area.get("id") and area["id"] not in {a["id"] for a in areas}:
                areas.append({**area, "files": files})
        if areas:
            return areas, list(mapped.get("invariants", [])), "map"
    return [dict(a) for a in config["seed_areas"]], [], "seed"


# ---------------------------------------------------------------- report
def write_report(out: Path, config: dict, summary: dict, candidates: list[dict],
                 triage: dict | None) -> Path:
    lines = [f"# Hunt report: {config['title']}", "",
             f"Scope commit `{config['scope_commit']}`. Areas from {summary['areas_from']}: "
             f"{len(summary['areas'])}. Hunters: {', '.join(summary['hunters'])}. "
             f"Hunt tasks: {summary['hunt_counts']}. Candidates: {len(candidates)}.", "",
             "These are leads, not findings. Every one needs a proof of concept at the scope "
             "commit before it is worth submitting, and you submit it yourself.", ""]
    if triage:
        lines += ["## Triage", "", triage.get("summary", ""), ""]
        for finding in triage.get("findings", []):
            lines += [f"### {finding.get('rank')}. {finding.get('title')} ({finding.get('verdict')})", "",
                      f"- Candidates: {', '.join(finding.get('candidate_ids', []))}",
                      f"- Location: {finding.get('location')}", "",
                      f"**Attack path.** {finding.get('attack_path')}", "",
                      f"**Why it is new.** {finding.get('why_new')}", "",
                      f"**Proof of concept plan.** {finding.get('poc_plan')}", "",
                      f"**Uncertain.** {finding.get('reviewer_notes')}", ""]
        if triage.get("rejected"):
            lines += ["## Rejected by triage", ""]
            lines += [f"- {r.get('candidate_id')}: {r.get('reason')}" for r in triage["rejected"]]
            lines.append("")
    elif candidates:
        lines += ["## Triage did not complete", "", "Review the candidates below directly.", ""]
    else:
        lines += ["## No candidates", "", "No hunter reported a Critical candidate. See what each "
                  "checked in results/hunts.json.", ""]
    if candidates:
        lines += ["## All candidates", ""]
        for c in candidates:
            lines += [f"### {c['id']} {c.get('title')} ({c['area']}, {c['model']}, {c.get('confidence')} confidence)",
                      "", f"- Location: {c.get('location')}", f"- Root cause: {c.get('root_cause')}",
                      f"- Attack path: {c.get('attack_path')}", f"- Preconditions: {c.get('preconditions')}",
                      f"- Impact: {c.get('impact')} ({c.get('funds_at_risk')})",
                      f"- Why new: {c.get('why_new')}", f"- PoC outline: {c.get('poc_outline')}", ""]
    if summary.get("failed_tasks"):
        lines += ["## Tasks that did not complete", ""]
        lines += [f"- {t['title']}: {t['status']}" for t in summary["failed_tasks"]]
        lines.append("")
    path = out / "report.md"
    path.write_text("\n".join(lines))
    return path


# ---------------------------------------------------------------- main
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="approve the model calls this run makes")
    parser.add_argument("--contest", required=True, type=Path, help="contest.json")
    parser.add_argument("--models", required=True, type=Path, help="models.json: who does what")
    parser.add_argument("--repo", required=True, type=Path, help="local clone of the contest repository")
    parser.add_argument("--output", type=Path, help="new directory for this run (default: hunt-<time>)")
    parser.add_argument("--skip-map", action="store_true", help="hunt the contest's seed areas directly")
    parser.add_argument("--areas", help="comma-separated area ids to hunt (default: all)")
    parser.add_argument("--hunters", help="comma-separated hunter names (default: all in models.json)")
    parser.add_argument("--max-tokens", type=int, help="token budget for the whole run")
    parser.add_argument("--stage-timeout-hours", type=float, default=6.0)
    parser.add_argument("--lease-seconds", type=float, default=60.0)
    parser.add_argument("--offline", action="store_true", help="do not download extra documents")
    args = parser.parse_args(argv)
    if not args.yes:
        parser.error("pass --yes to approve the model calls this run makes")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    contest_dir = args.contest.resolve().parent
    config = json.loads(args.contest.read_text())
    models = json.loads(args.models.read_text())
    specs, stages = models["models"], models["stages"]
    hunters = list(stages["hunt"])
    if args.hunters:
        hunters = [h for h in args.hunters.split(",") if h in hunters]
        if not hunters:
            raise SystemExit("--hunters matched none of the hunters in models.json")
    for name in [stages.get("map"), stages.get("triage"), *hunters]:
        if name and name not in specs:
            raise SystemExit(f"models.json names {name!r} in stages but not in models")
    out = (args.output or Path(f"hunt-{time.strftime('%Y%m%d-%H%M%S')}")).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / "results").mkdir()
    print(f"[contest] preparing workspace in {out}", flush=True)
    workspace = prepare_workspace(config, contest_dir, args.repo.expanduser().resolve(), out, args.offline)
    contest = workspace / "contest"

    harness = Harness(out, out=sys.stdout, lease_seconds=args.lease_seconds)
    workers = Workers(harness, out, workspace, heartbeat=args.lease_seconds / 4)
    stage_timeout = args.stage_timeout_hours * 3600
    summary: dict = {"contest": config["name"], "hunters": hunters, "output": str(out)}
    candidates: list[dict] = []
    triage = None
    try:
        harness.start_bus()
        harness.start_pm()
        correlation = None

        def submit(title, capability, context):
            nonlocal correlation
            task_id, corr = harness.submit(title, capability=capability, context=context,
                                           max_retries=1, correlation_id=correlation, quiet=True)
            if correlation is None:
                correlation = corr
                if args.max_tokens:
                    harness.cli(["policy", corr, "--max-tokens", str(args.max_tokens),
                                 "--reserve-tokens-per-assignment", str(max(1, args.max_tokens // 50)),
                                 "--reason", "contest hunt token budget"], quiet=True)
            return task_id

        def inline_for(name, files):
            return files if specs[name]["runner"] == RUNNER_ONLY_INLINE else []

        base_inline = ["contest/rules.md", "contest/known_issues.md", "contest/primer.md"]
        mapped = None
        if not args.skip_map and stages.get("map"):
            mapper = stages["map"]
            workers.start(f"{mapper}-map", "map", specs[mapper])
            (contest / "briefs" / "map.md").write_text(briefs.map_brief(config["seed_areas"]))
            task = submit("Map the changed code into hunting areas", f"{mapper}-map",
                          {"stage": "map", "brief": "contest/briefs/map.md",
                           "inline_files": inline_for(mapper, base_inline + ["contest/scope.diff"])})
            states = wait_tasks(harness, [task], stage_timeout, "mapping")
            if states[task].get("status") == "completed":
                mapped = completed_results(harness).get(task)
                (out / "results" / "map.json").write_text(json.dumps(mapped, indent=2))
        areas, invariants, summary["areas_from"] = choose_areas(config, mapped)
        if args.areas:
            wanted = set(args.areas.split(","))
            areas = [a for a in areas if a["id"] in wanted]
            if not areas:
                raise SystemExit("--areas matched none of the areas")
        summary["areas"] = [{"id": a["id"], "title": a["title"]} for a in areas]
        (contest / "areas.json").write_text(json.dumps({"areas": areas, "invariants": invariants}, indent=2))

        for hunter in hunters:
            workers.start(f"{hunter}-hunt", "hunt", specs[hunter])
        hunt_tasks = {}
        for area in areas:
            diff = write_area_diff(workspace, area)
            brief = f"contest/briefs/hunt-{area['id']}.md"
            (workspace / brief).write_text(briefs.hunt_brief(area, invariants))
            for hunter in hunters:
                task = submit(f"Hunt {area['id']} {area['title']} ({hunter})", f"{hunter}-hunt",
                              {"stage": "hunt", "area": area["id"], "brief": brief,
                               "inline_files": inline_for(hunter, base_inline + [diff])})
                hunt_tasks[task] = (area["id"], hunter, area["title"])
        states = wait_tasks(harness, list(hunt_tasks), stage_timeout, "hunting")
        results = completed_results(harness)
        hunts = [{"task_id": t, "area": a, "model": h, "status": states[t].get("status"),
                  "result": results.get(t, {})} for t, (a, h, _) in hunt_tasks.items()]
        (out / "results" / "hunts.json").write_text(json.dumps(hunts, indent=2))
        summary["hunt_counts"] = {s: sum(h["status"] == s for h in hunts)
                                  for s in sorted({h["status"] for h in hunts})}
        summary["failed_tasks"] = [{"title": f"Hunt {a} ({h})", "status": states[t].get("status")}
                                   for t, (a, h, _) in hunt_tasks.items()
                                   if states[t].get("status") != "completed"]
        candidates = briefs.number_candidates([h for h in hunts if h["status"] == "completed"])
        (contest / "candidates.json").write_text(briefs.candidates_json(candidates))
        (out / "results" / "candidates.json").write_text(briefs.candidates_json(candidates))

        if candidates and stages.get("triage"):
            judge = stages["triage"]
            workers.start(f"{judge}-triage", "triage", specs[judge])
            (contest / "briefs" / "triage.md").write_text(briefs.triage_brief())
            task = submit("Triage the hunt candidates", f"{judge}-triage",
                          {"stage": "triage", "brief": "contest/briefs/triage.md",
                           "inline_files": inline_for(judge, base_inline + ["contest/candidates.json"])})
            states = wait_tasks(harness, [task], stage_timeout, "triage")
            if states[task].get("status") == "completed":
                triage = completed_results(harness).get(task)
                (out / "results" / "triage.json").write_text(json.dumps(triage, indent=2))
            else:
                summary.setdefault("failed_tasks", []).append(
                    {"title": "Triage", "status": states[task].get("status")})
        if correlation:
            summary["workflow"] = harness.workflow_state(correlation).get("budget")
        report = write_report(out, config, summary, candidates, triage)
        print(f"[contest] report: {report}", flush=True)
        return 0
    finally:
        try:
            (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
        finally:
            harness.stop_all()


if __name__ == "__main__":
    sys.exit(main())
