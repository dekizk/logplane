"""Review a codebase with a fleet of Hermes workers, and survive what goes wrong.

    python -m examples.hermes.code_review --provider PROVIDER --model MODEL [--path DIR]

Splits the source files under ``--path`` (default: logplane's own package)
into chunks and fans them out to several Hermes Agent workers in parallel.
Each review is a real, paid model call. Along the way:

- one reviewer is killed (``kill -9``) in the middle of a model call, its
  lease expires, and its chunk is reassigned to a surviving worker;
- a cost cap, set from the first review's real cost, stops new work part-way
  through, and a human raises it;
- Hermes merges and ranks every chunk's findings into one list, in batches
  when there are many chunks;
- a Python "filer" agent asks a human which severities to file, then records
  them in a local tracker keyed by ``effect_id``, so a retry cannot file a
  finding twice, and writes REVIEW.md.

Point ``--path`` at your own repository to review it instead. Costs scale with
``--max-chunks`` and the model; the defaults review about 100 KB of source.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional, TextIO

from examples.hermes import fleet
from logplane.demo import (DemoError, Harness, deliver_effect, latest_decision, normalize_choice,
                           plain)
from logplane.executors import AssignmentContext, Blocked, Completed, PermanentFailure

SEVERITIES = ("high", "medium", "low")
MERGE_BATCH = 8  # reviews per merge task, so merged inputs stay under the 32 KB dependency limit
FILE_CHOICES = ("high", "medium", "all", "none")
TRACKER = "findings.db"
REPORT = "REVIEW.md"
DEFAULT_PATH = Path(__file__).resolve().parents[2] / "src" / "logplane"

REVIEW_INSTRUCTIONS = (
    "Review this chunk of source code as a careful senior engineer. Look for bugs, "
    "race conditions, error handling gaps, security issues, and misleading names or "
    "comments. Report at most 5 findings, most important first. Return "
    "result.findings as a list of objects with keys: file (exactly the context's file "
    "value), line (an integer within the chunk's line range), severity (high, medium, "
    "or low), title (under 12 words), and detail (under 40 words, concrete). Return an "
    "empty list if the code is fine. "
    "Do not report style nits."
)
MERGE_INSTRUCTIONS = (
    "You are given the findings from several reviewers, one per upstream dependency, "
    "each with file names and line numbers. Merge duplicates, drop anything that is "
    "not a real problem, and return the 10 most important findings ranked, as "
    "result.findings: a list of objects with keys file, line, severity (high, medium, "
    "or low), title (under 12 words), and detail (under 40 words). Keep the file and "
    "line of the strongest instance when merging."
)


# -- the Python filer: the only agent with an irreversible job ---------------

class FilerExecutor:
    """Files chosen findings once each and writes the report after a human decides."""

    def __init__(self, workdir: Path):
        self.workdir = workdir

    def execute(self, assignment: AssignmentContext):
        if assignment.context.get("agent_bus_conformance_probe"):
            return Completed("probe ok")
        if assignment.context.get("stage") != "file":
            return PermanentFailure("unknown_stage", "unexpected stage")
        if not assignment.dependencies:
            return PermanentFailure("missing_input", "filing needs the merged findings")
        # Assignment data arrives as immutable mappings; copy it to plain JSON first.
        findings = plain(assignment.dependencies[-1].get("result", {})).get("findings")
        if not isinstance(findings, list):
            return PermanentFailure("missing_findings", "the merged result has no findings list")
        findings = [f for f in findings if isinstance(f, dict)]
        counts = {s: sum(1 for f in findings if f.get("severity") == s) for s in SEVERITIES}
        question = (f"{len(findings)} findings ({counts['high']} high, {counts['medium']} "
                    f"medium, {counts['low']} low). File which? (high/medium/all/none)")
        decision = latest_decision(assignment)
        if decision is None:
            return Blocked(question)
        choice = normalize_choice(decision, FILE_CHOICES)
        if choice is None:
            return Blocked(f"{question} ({json.dumps(decision)} is not one of: "
                           f"{', '.join(FILE_CHOICES)}.)")
        allowed = {"high": {"high"}, "medium": {"high", "medium"},
                   "all": set(SEVERITIES), "none": set()}[choice]
        chosen = [f for f in findings if f.get("severity") in allowed]

        filed, skipped = [], []
        for index, finding in enumerate(chosen, start=1):
            key = f"{finding.get('file')}:{finding.get('line')}:{index}"
            # Same key on every attempt of this task; the tracker accepts it once.
            if deliver_effect(self.workdir / TRACKER, assignment.effect_id(f"file:{key}"),
                              {"attempt": assignment.attempt, **finding}):
                filed.append(key)
            else:
                skipped.append(key)
        (self.workdir / REPORT).write_text(render_report(findings, choice), encoding="utf-8")
        return Completed(
            f"Filed {len(filed)} finding(s), skipped {len(skipped)} already filed; wrote {REPORT}",
            {"filed": len(filed), "skipped": len(skipped), "choice": choice, "report": REPORT},
        )


def format_findings(findings: list[dict]) -> str:
    lines = []
    for finding in findings:
        lines.append(f"- **{finding.get('severity', '?')}** `{finding.get('file', '?')}:"
                     f"{finding.get('line', '?')}` {finding.get('title', '')}")
        if finding.get("detail"):
            lines.append(f"  {finding['detail']}")
    return "\n".join(lines)


def render_report(findings: list[dict], choice: str) -> str:
    header = ["# Review findings", "", f"{len(findings)} merged findings; filed: {choice}.", ""]
    return "\n".join(header) + format_findings(findings) + "\n"


# -- splitting the source ----------------------------------------------------

# Never sent for review, whatever --include says: tooling, dependencies, build
# output, and anything hidden. A git checkout also honours its .gitignore.
EXCLUDED_DIRS = frozenset({
    "__pycache__", "node_modules", "venv", "env", "build", "dist", "target",
    "site-packages", "vendor", "third_party",
})
EXCLUDED_FILES = ("*.min.*", "*.lock", "*.pem", "*.key", "*secret*", "*credential*", "*.env")


def select_files(root: Path, include: str) -> list[Path]:
    """Files under root that match ``include`` and are safe to send for review.

    In a git checkout, only files git tracks or does not ignore are eligible,
    so .gitignore applies. Hidden directories and files (.venv, .git, .env),
    common dependency and build directories, and secret-looking names are
    always skipped.
    """
    tracked = _git_visible_files(root)
    chosen = []
    for path in sorted(root.rglob(include)):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root)
        parts = relative.parts
        if any(part.startswith(".") for part in parts) or any(part in EXCLUDED_DIRS for part in parts[:-1]):
            continue
        if any(fnmatch.fnmatch(path.name.lower(), pattern) for pattern in EXCLUDED_FILES):
            continue
        if tracked is not None and relative.as_posix() not in tracked:
            continue
        chosen.append(path)
    return chosen


def _git_visible_files(root: Path) -> Optional[set[str]]:
    """Paths git would not ignore, relative to root; None outside a checkout."""
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            capture_output=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return {item.decode("utf-8", "replace") for item in completed.stdout.split(b"\0") if item}


def chunk_files(root: Path, include: str, chunk_bytes: int, max_chunks: int) -> list[dict]:
    """Split selected files into line-aligned chunks of at most chunk_bytes.

    Files are taken largest first and their chunks interleaved, so a small
    ``max_chunks`` still samples several files rather than one file's start.
    """
    files = sorted(select_files(root, include), key=lambda p: (-p.stat().st_size, str(p)))
    per_file = []
    for path in files:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        chunks, start, size, buffer = [], 1, 0, []
        for number, line in enumerate(lines, start=1):
            if buffer and size + len(line.encode("utf-8")) > chunk_bytes:
                chunks.append(_chunk(path, root, start, number - 1, buffer))
                start, size, buffer = number, 0, []
            buffer.append(line)
            size += len(line.encode("utf-8"))
        if buffer:
            chunks.append(_chunk(path, root, start, len(lines), buffer))
        per_file.append(chunks)
    interleaved = []
    for round_index in range(max(map(len, per_file), default=0)):
        for chunks in per_file:
            if round_index < len(chunks):
                interleaved.append(chunks[round_index])
    return interleaved[:max_chunks]


def _chunk(path: Path, root: Path, start: int, end: int, buffer: list[str]) -> dict:
    return {"file": str(path.relative_to(root)), "start_line": start, "end_line": end,
            "code": "".join(buffer)}


# -- the scenario --------------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m examples.hermes.code_review",
        description="Fan a code review out to several Hermes workers under logplane",
    )
    fleet.add_hermes_arguments(parser, default_pace=2.0)
    parser.set_defaults(timeout=900.0)  # a careful review of an 8 KB chunk can take minutes
    parser.add_argument("--path", type=Path, default=DEFAULT_PATH,
                        help="directory to review (default: logplane's own source)")
    parser.add_argument("--include", default="*.py", help="glob for files to review")
    parser.add_argument("--chunk-bytes", type=int, default=8_000,
                        help="largest chunk handed to one review (inline context limit is 16 KB)")
    parser.add_argument("--max-chunks", type=int, default=12, help="number of review tasks")
    parser.add_argument("--workers", type=int, default=4, help="Hermes reviewers to start")
    parser.add_argument("--crash", action=argparse.BooleanOptionalAction, default=True,
                        help="kill one reviewer mid-call to show reassignment")
    parser.add_argument("--cap", action=argparse.BooleanOptionalAction, default=True,
                        help="set a cost cap from the first review's cost so it trips mid-run")
    return parser.parse_args(argv)


def run(args: argparse.Namespace, *, out: TextIO = sys.stdout,
        stdin: TextIO = sys.stdin) -> int:
    if args.workers < 2 and args.crash:
        print("code_review: --crash needs at least 2 workers", file=out)
        return 2
    return fleet.run_example(args, _scenario, prefix="hermes-review", out=out, stdin=stdin)


def _scenario(h: Harness, args: argparse.Namespace) -> None:
    root = args.path.expanduser().resolve()
    chunks = chunk_files(root, args.include, args.chunk_bytes, args.max_chunks)
    if not chunks:
        raise DemoError(f"no files matching {args.include!r} under {root}")
    total_kb = sum(len(c["code"].encode("utf-8")) for c in chunks) / 1024
    files = sorted({c["file"] for c in chunks})

    h.title("A code review by a fleet of real agents")
    h.say(f"{len(chunks)} chunks ({total_kb:.0f} KB) from {len(files)} file(s) under {root} "
          f"will be sent to {args.provider} / {args.model}, reviewed by {args.workers} Hermes "
          "workers in parallel. Every review is a real model call. A Python agent files the "
          "findings at the end, after you choose which.")
    h.say("What will be sent (hidden, ignored, dependency, build, and secret-looking files "
          "are skipped):")
    h.block("\n".join(f"{c['file']} lines {c['start_line']}-{c['end_line']} "
                      f"({len(c['code'].encode('utf-8')) / 1024:.1f} KB)" for c in chunks))
    if h.ask("Send these chunks for review? (yes/no)", default="yes", choices=("yes", "no")) == "no":
        raise DemoError("nothing was sent; adjust --path, --include, or --max-chunks and rerun")
    h.start_bus()
    h.start_pm()
    reviewers = [f"reviewer-{n}" for n in range(1, args.workers + 1)]
    for name in reviewers:
        fleet.start_hermes_worker(h, args, name, capability="review")
    fleet.start_python_worker(h, "examples.hermes.code_review", "filer")

    reviews, workflow = [], None
    for chunk in chunks:
        title = f"Review {chunk['file']} lines {chunk['start_line']}-{chunk['end_line']}"
        task, workflow = h.submit(title, capability="review", max_retries=2,
                                  correlation_id=workflow, quiet=True,
                                  context={"stage": "review", **chunk,
                                           "instructions": REVIEW_INSTRUCTIONS})
        reviews.append(task)
    h.emit(f"  Submitted {len(reviews)} review tasks in workflow {workflow}")
    merge, stages = _submit_merges(h, reviews)
    filing, _ = h.submit("File the findings", depends_on=[merge], capability="file",
                         max_retries=2, context={"stage": "file"})
    h.cli(["policy", workflow, "--max-active-assignments", str(args.workers),
           "--reason", "one review per worker"])
    deadline = args.timeout + 120

    if args.crash:
        _crash_a_reviewer(h, reviewers, reviews)
    first_cost = None
    if args.cap:
        first_cost = _trip_and_raise_cap(h, workflow, reviews, deadline)

    h.say(f"Waiting for the remaining reviews and the merge{stages} (real calls; this takes "
          "a while)...")
    for task in reviews:
        fleet.wait_done(h, task, deadline)
    fleet.wait_done(h, merge, deadline)
    completed = h.find_event("task.completed", task_id=merge) or {}
    findings = [f for f in (completed.get("payload", {}).get("result") or {}).get("findings", [])
                if isinstance(f, dict)]
    h.say(f"Hermes merged everything into {len(findings)} ranked findings:")
    h.block(format_findings(findings) or "(none)")

    needed = h.wait_for("decision.needed", task_id=filing, timeout=120)
    h.say("The filer will not record anything until you choose. The question and your answer "
          "are both events:")
    h.view(["decisions"])
    answer = h.ask("File which findings? (high/medium/all/none)", default="medium",
                   choices=FILE_CHOICES)
    h.cli(["decide", str(filing), "--decision-event", str(needed["id"]),
           "--value", json.dumps(answer)])
    h.wait_status(filing, "completed", timeout=120)
    h.pause(0.5)
    h.view(["task", str(filing)])
    h.view(["workflow", workflow])
    h.view(["workflow", workflow, "--mermaid"])

    state = h.workflow_state(workflow)
    usage, budget = state["telemetry"]["usage"], state["budget"]
    expired = len(h.events(["task.assignment_expired"]))
    points = [
        f"{state['telemetry']['model']['completed']} real model calls: "
        f"{usage['total_tokens']:,} tokens, ${usage['cost_usd']:.4f}, recorded per call in the log.",
    ]
    if args.crash:
        points.append(f"A reviewer died mid-call; {expired} lease(s) expired and the work was "
                      "reassigned. The dead call never reported usage, so its attempt is charged "
                      "its reservation, not guessed at.")
    if first_cost:
        points.append("The cap stopped new reviews part-way; raising it was an event with a "
                      f"reason, and the budget line (${budget['cost_usd']:.4f}) shows the result.")
    points.append("Filing waited for your choice, and each finding's key is stable across "
                  f"attempts, so a retry cannot file it twice. The report is in {REPORT}.")
    h.recap(points)


def _submit_merges(h: Harness, reviews: list[int]) -> tuple[int, str]:
    """Merge in batches, then merge the batches, so inputs stay within limits.

    Returns the final merge task and a description of the stages for narration.
    """
    context = {"stage": "merge", "instructions": MERGE_INSTRUCTIONS}
    if len(reviews) <= MERGE_BATCH:
        merge, _ = h.submit("Merge and rank the findings", depends_on=reviews,
                            capability="review", max_retries=2, context=context)
        return merge, ""
    batches = [reviews[i:i + MERGE_BATCH] for i in range(0, len(reviews), MERGE_BATCH)]
    partial = []
    for number, batch in enumerate(batches, start=1):
        task, _ = h.submit(f"Merge findings, batch {number} of {len(batches)}",
                           depends_on=batch, capability="review", max_retries=2,
                           context=context, quiet=True)
        partial.append(task)
    merge, _ = h.submit("Merge and rank the findings", depends_on=partial,
                        capability="review", max_retries=2, context=context)
    return merge, f" ({len(batches)} batch merges, then a final merge)"


def _crash_a_reviewer(h: Harness, reviewers: list[str], reviews: list[int]) -> None:
    def two_running():
        started = {e["actor"] for e in h.events(["task.started"]) if e["actor"] in reviewers}
        return started if len(started) >= 2 else None

    running = h.wait_until(two_running, 120, "two reviewers to start model calls")
    victim = sorted(running)[-1]
    h.pause(2.0)
    task = next((e["payload"]["task_id"] for e in reversed(h.events(["task.started"]))
                 if e["actor"] == victim), None)
    h.kill(victim)
    h.say(f"{victim} was part-way through a real model call on task {task} when it was "
          "killed (kill -9). Its lease expires, the chunk goes to another reviewer, and "
          "nothing it would have said is trusted.")
    h.wait_for("task.assignment_expired", task_id=task, timeout=60)


def _trip_and_raise_cap(h: Harness, workflow: str, reviews: list[int], deadline: float) -> Optional[float]:
    def first_done():
        state = h.workflow_state(workflow)
        done = [t for t in state["tasks"] if t["status"] == "completed" and t["task_id"] in reviews]
        return state if done else None

    state = h.wait_until(first_done, deadline, "the first review to finish")
    usage = state["telemetry"]["usage"]
    per_review = usage["cost_usd"] / max(1, state["telemetry"]["model"]["completed"])
    if per_review <= 0:
        h.say("Your provider reports no cost, so the cap step is skipped; tokens are still "
              "counted per call.")
        return None
    cap = round(per_review * len(reviews) * 0.6, 4)
    reserve = round(per_review, 4)
    h.say(f"The first review cost ${per_review:.4f}. A cap of ${cap:.4f} (about 60% of what "
          f"{len(reviews)} reviews would cost) now gates new work, reserving ${reserve:.4f} "
          "per running review until its real cost is reported:")
    h.cli(["policy", workflow, "--max-cost", f"{cap:.4f}",
           "--reserve-cost-per-assignment", f"{reserve:.4f}", "--reason", "trial cap"])

    def capped():
        state = h.workflow_state(workflow)
        return state if state["budget"].get("blocked") else None

    # Running reviews carry on; the cap only refuses to admit new ones.
    state = h.wait_until(capped, deadline, "the cap to stop new work")
    done = sum(t["status"] == "completed" for t in state["tasks"])
    running = sum(t["status"] in {"assigned", "started"} for t in state["tasks"])
    h.say(f"New work stopped after {done} reviews at ${state['budget']['cost_usd']:.4f} "
          f"committed; {running} running review(s) will finish, and waiting ones say why:")
    waiting = next((t["task_id"] for t in state["tasks"] if t["status"] == "open"), None)
    if waiting:
        h.view(["explain", str(waiting)])
    raised = round(per_review * (len(reviews) * 1.5 + 6), 4)
    answer = h.ask(f"Raise the cap to ${raised:.4f}? (yes/no)", default="yes", choices=("yes", "no"))
    if answer == "no":
        raise DemoError("the cap was left in place; the remaining reviews stay queued")
    h.cli(["policy", workflow, "--max-cost", f"{raised:.4f}",
           "--reserve-cost-per-assignment", f"{reserve:.4f}", "--reason", "approved: finish the review"])
    return per_review


def main(argv: Optional[list[str]] = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "_worker":
        return fleet.run_python_worker(argv[1:], "filer", "file", FilerExecutor)
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
