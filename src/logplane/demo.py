"""Scripted, self-contained demonstrations of logplane.

``logplane demo <scenario>`` starts a real bus, PM, and workers as separate
processes in a fresh directory, narrates a short story against them, and keeps
the resulting event history for inspection. Crashes are genuine SIGKILLs of
those processes, and every view is the normal CLI projection over the log.

The scenario agents below are deterministic stand-ins: no model, network, or
credentials are involved. Replace any stage with a real agent through the
normal adapter contract (see ``examples/audit_triage/README.md``).
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, TextIO

from logplane.executors import AssignmentContext, Blocked, Completed

# --------------------------------------------------------------------------
# Scenario agents (these run inside the worker subprocesses)
# --------------------------------------------------------------------------

# Fictional "Lumen" protocol. Findings are canned so every run is identical.
REVIEW_FINDINGS: dict[str, list[dict[str, Any]]] = {
    "Vault.sol": [
        {
            "title": "First depositor can inflate the share price and round later deposits to zero",
            "root_cause": "share-inflation",
            "severity": "High",
            "contested": False,
        },
    ],
    "PriceOracle.sol": [
        {
            "title": "latestRoundData() answer is used without checking updatedAt",
            "root_cause": "stale-oracle-price",
            "severity": "Medium",
            "contested": True,
        },
    ],
    "Router.sol": [
        {
            "title": "quote() trusts a PriceOracle price that can be stale",
            "root_cause": "stale-oracle-price",
            "severity": "Medium",
            "contested": True,
        },
        {
            "title": "swapExactIn() has no minimum-output bound",
            "root_cause": "missing-slippage-bound",
            "severity": "Medium",
            "contested": False,
        },
    ],
}

SEVERITIES = ("High", "Medium")
APPROVAL_CHOICES = ("approve", "deny")
INBOX_FILE = "contest-inbox.db"
REFUND_LEDGER = "refunds.db"
REPORT_FILE = "report.md"


def latest_decision(assignment: AssignmentContext) -> Optional[Any]:
    if not assignment.decisions:
        return None
    return assignment.decisions[-1].get("decision")


def plain(value: Any) -> Any:
    """Copy immutable assignment data into ordinary JSON values."""
    if isinstance(value, Mapping):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def upstream_results(assignment: AssignmentContext) -> list[dict[str, Any]]:
    return [plain(dependency.get("result", {})) for dependency in assignment.dependencies]


def dedupe_findings(reviews: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Merge reviewer findings that share a root cause, in a stable order."""
    merged: dict[str, dict[str, Any]] = {}
    for review in sorted(reviews, key=lambda value: value.get("contract", "")):
        for finding in review.get("findings", []):
            key = finding["root_cause"]
            entry = merged.setdefault(
                key,
                {
                    "root_cause": key,
                    "title": finding["title"],
                    "severity": finding["severity"],
                    "contested": bool(finding.get("contested")),
                    "locations": [],
                },
            )
            entry["locations"].append(review.get("contract", "?"))
            entry["contested"] = entry["contested"] or bool(finding.get("contested"))
    ordered = list(merged.values())
    for number, finding in enumerate(ordered, start=1):
        finding["id"] = f"AUD-{number}"
    return ordered


def normalize_choice(answer: Any, choices: Iterable[str]) -> Optional[str]:
    """Match an answer to one of the choices, ignoring case and spacing."""
    text = str(answer).strip().casefold()
    return next((choice for choice in choices if choice.casefold() == text), None)


def triage_question(findings: Iterable[Mapping[str, Any]]) -> Optional[str]:
    contested = [finding for finding in findings if finding.get("contested")]
    if not contested:
        return None
    finding = contested[0]
    return (
        f"{finding['id']} ({', '.join(finding['locations'])}): {finding['title']}. "
        f"Reviewers suggest {finding['severity']}. Severity: High or Medium?"
    )


class ScenarioExecutor:
    """One executor for every scenario stage, selected by ``context.stage``.

    When a task's context carries ``model_usage``, the stage reports one
    stand-in model call through the normal telemetry sink, so workflows show
    real token and cost accounting (provider ``demo``, model ``stand-in``).
    """

    def __init__(self, name: str, workdir: Path, telemetry=None):
        self.name = name
        self.workdir = Path(workdir)
        self.telemetry = telemetry

    def execute(self, assignment: AssignmentContext):
        context = assignment.context
        seconds = float(context.get("work_seconds", 0.5))
        if assignment.attempt > 1 and "retry_work_seconds" in context:
            seconds = float(context["retry_work_seconds"])
        stage = str(context.get("stage", "work"))
        handler = getattr(self, f"stage_{stage.replace('-', '_')}", None)
        if handler is None:
            raise ValueError(f"unknown demo stage: {stage}")
        call = self._start_model_call(assignment, context)
        if stage != "submit":
            time.sleep(seconds)
        outcome = handler(assignment, context)
        self._finish_model_call(assignment, context, call)
        return outcome

    def _start_model_call(self, assignment, context) -> Optional[dict]:
        if self.telemetry is None or "model_usage" not in context:
            return None
        call = {"invocation_id": f"{assignment.assignment_id}:model",
                "started": time.monotonic()}
        try:  # telemetry is best-effort and must never change the outcome
            self.telemetry.model_started(assignment, invocation_id=call["invocation_id"],
                                         provider="demo", model="stand-in")
        except Exception:
            pass
        return call

    def _finish_model_call(self, assignment, context, call: Optional[dict]) -> None:
        if call is None:
            return
        try:
            self.telemetry.model_completed(
                assignment, invocation_id=call["invocation_id"], provider="demo",
                model="stand-in", duration_ms=(time.monotonic() - call["started"]) * 1000,
                usage=plain(context["model_usage"]))
        except Exception:
            pass

    def stage_work(self, assignment, context):
        return Completed(summary=f"{self.name} finished: {assignment.goal}")

    def stage_approval(self, assignment, context):
        """Ask before an irreversible tool call, then make it at most once."""
        tool = str(context["tool"])
        arguments = plain(context.get("arguments", {}))
        call = str(context.get("call") or tool)
        question = f"The support agent wants to call {call}. Approve or deny?"
        decision = latest_decision(assignment)
        if decision is None:
            return Blocked(question)
        choice = normalize_choice(decision, APPROVAL_CHOICES)
        if choice is None:
            return Blocked(f"{question} ({json.dumps(decision)} is not one of: "
                           f"{', '.join(APPROVAL_CHOICES)}.)")
        if choice == "deny":
            return Completed(summary=f"Human denied {call}; nothing was executed",
                             result={"tool": tool, "outcome": "denied"})
        # The key is the same on every attempt of this task. The simulated
        # payments system enforces it, the way a real API enforces an
        # idempotency key, so overlapping attempts cannot both refund.
        effect = assignment.effect_id(f"tool:{tool}")
        record = {"tool": tool, "arguments": arguments,
                  "approved_by": assignment.decisions[-1].get("actor"),
                  "attempt": assignment.attempt}
        if deliver_effect(self.workdir / REFUND_LEDGER, effect, record):
            summary = f"Executed {call} after human approval"
        else:
            summary = f"{call} was already executed by an earlier attempt; not repeated"
        return Completed(summary=summary, result={"tool": tool, "outcome": "executed"})

    def stage_reply(self, assignment, context):
        outcomes = [result.get("outcome") for result in upstream_results(assignment)
                    if "outcome" in result]
        if "executed" in outcomes:
            message = "Sent the customer: your $120.00 refund is on its way."
        else:
            message = "Sent the customer: a teammate will follow up about your refund."
        return Completed(summary=message, result={"refund": outcomes[0] if outcomes else None})

    def stage_scope(self, assignment, context):
        contracts = plain(context["contracts"])
        return Completed(
            summary=f"Scoped {len(contracts)} contracts: {', '.join(contracts)}",
            result={"contracts": contracts},
        )

    def stage_review(self, assignment, context):
        contract = str(context["contract"])
        findings = plain(REVIEW_FINDINGS.get(contract, []))
        return Completed(
            summary=f"{self.name} reviewed {contract}: {len(findings)} finding(s)",
            result={"contract": contract, "findings": findings},
        )

    def stage_dedupe(self, assignment, context):
        reviews = upstream_results(assignment)
        raw = sum(len(review.get("findings", [])) for review in reviews)
        findings = dedupe_findings(reviews)
        return Completed(
            summary=f"Merged {raw} reviewer findings into {len(findings)} unique issues",
            result={"findings": findings},
        )

    def stage_triage(self, assignment, context):
        findings = [item for result in upstream_results(assignment)
                    for item in result.get("findings", [])]
        question = triage_question(findings)
        decision = latest_decision(assignment)
        severity = None
        if question is not None:
            if decision is None:
                return Blocked(question)
            severity = normalize_choice(decision, SEVERITIES)
            if severity is None:
                # An answer from `logplane decide` is free-form: ask again.
                return Blocked(f"{question} ({json.dumps(decision)} is not one of: "
                               f"{', '.join(SEVERITIES)}.)")
        for finding in findings:
            if finding.get("contested"):
                finding["severity"] = severity
                finding["contested"] = False
        counts = {}
        for finding in findings:
            counts[finding["severity"]] = counts.get(finding["severity"], 0) + 1
        breakdown = ", ".join(f"{count} {severity}" for severity, count in sorted(counts.items()))
        return Completed(summary=f"Triaged {len(findings)} issues: {breakdown}",
                         result={"findings": findings})

    def stage_report(self, assignment, context):
        findings = [item for result in upstream_results(assignment)
                    for item in result.get("findings", [])]
        lines = ["# Lumen protocol: audit findings", ""]
        for finding in findings:
            lines.append(f"- **{finding['id']} [{finding['severity']}]** {finding['title']} "
                         f"({', '.join(finding['locations'])})")
        (self.workdir / REPORT_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")
        return Completed(summary=f"Wrote {REPORT_FILE} with {len(findings)} findings",
                         result={"report": REPORT_FILE, "findings": findings})

    def stage_submit(self, assignment, context):
        """Submit each finding once, even across crashed attempts.

        The inbox stands in for a contest platform that accepts an idempotency
        key. Each submission carries ``assignment.effect_id``, which stays the
        same on every retry of this task, and the platform refuses a key it has
        already seen, so a later or overlapping attempt cannot submit twice.
        """
        findings = [item for result in upstream_results(assignment)
                    for item in result.get("findings", [])]
        pause = float(context.get("pause_seconds", 0.0)) if assignment.attempt == 1 else 0.0
        inbox = self.workdir / INBOX_FILE
        submitted, skipped = [], []
        for finding in findings:
            effect = assignment.effect_id(f"submit-finding:{finding['id']}")
            record = {"finding": finding["id"], "severity": finding["severity"],
                      "title": finding["title"], "submitted_by": self.name,
                      "attempt": assignment.attempt}
            if not deliver_effect(inbox, effect, record):
                skipped.append(finding["id"])
                continue
            submitted.append(finding["id"])
            time.sleep(pause)
        return Completed(
            summary=(f"Submitted {len(submitted)} finding(s); skipped {len(skipped)} "
                     "already submitted by an earlier attempt"),
            result={"submitted": submitted, "skipped": skipped},
        )


def deliver_effect(path: Path, effect_id: str, record: Mapping[str, Any]) -> bool:
    """Perform a simulated external effect at most once per idempotency key.

    This stands in for the receiving system (a payments API, a contest
    platform). The uniqueness check and the write are one SQLite statement, so
    two overlapping attempts cannot both perform the effect. Checking a list
    first and writing afterwards would not be safe. Returns False for a key
    the system has already accepted.
    """
    entry = json.dumps({"effect_id": effect_id, **record}, sort_keys=True)
    with contextlib.closing(sqlite3.connect(path, timeout=30)) as conn:
        with conn:
            conn.execute("CREATE TABLE IF NOT EXISTS effects ("
                         "seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                         "effect_id TEXT NOT NULL UNIQUE, record TEXT NOT NULL)")
            cursor = conn.execute(
                "INSERT OR IGNORE INTO effects (effect_id, record) VALUES (?, ?)",
                (effect_id, entry))
            return cursor.rowcount == 1


def read_inbox(path: Path) -> list[dict[str, Any]]:
    """Return the effects a simulated receiver accepted, oldest first."""
    if not path.exists():
        return []
    with contextlib.closing(sqlite3.connect(path, timeout=30)) as conn:
        try:
            rows = conn.execute("SELECT record FROM effects ORDER BY seq").fetchall()
        except sqlite3.OperationalError:
            return []
    return [json.loads(row[0]) for row in rows]


def _worker_main(argv: list[str]) -> int:
    from logplane.client import BusClient
    from logplane.runtime import WorkerRuntime

    parser = argparse.ArgumentParser(prog="demo worker")
    parser.add_argument("--url", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--capability", action="append", default=[])
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument("--heartbeat", type=float, default=1.0)
    args = parser.parse_args(argv)
    import uuid

    from logplane.telemetry import BusTelemetrySink, ProducerIdentity
    from logplane.version import VERSION

    bus = BusClient(args.url, actor=args.name)
    instance_id = uuid.uuid4().hex
    telemetry = BusTelemetrySink(bus, producer=ProducerIdentity(
        implementation="logplane-demo", instance_id=instance_id, version=VERSION))
    runtime = WorkerRuntime(
        bus,
        name=args.name,
        instance_id=instance_id,
        executor=ScenarioExecutor(args.name, Path(args.workdir), telemetry),
        capacity=args.capacity,
        capabilities=args.capability,
        heartbeat_seconds=args.heartbeat,
        log=lambda message: print(message, flush=True),
    )
    try:
        runtime.run()
    except KeyboardInterrupt:
        runtime.stop()
    return 0


# --------------------------------------------------------------------------
# Harness (runs in the demo process)
# --------------------------------------------------------------------------

class DemoError(RuntimeError):
    pass


SHOWN_TOPICS = {
    "agent.registered": "registered",
    "task.created": "created",
    "task.assigned": "assigned",
    "task.started": "started",
    "task.completed": "completed",
    "task.blocked": "blocked",
    "decision.needed": "decision needed",
    "decision.made": "decision made",
    "task.assignment_expired": "assignment expired",
    "task.attempt_failed": "attempt failed",
    "task.failed": "FAILED",
    "task.dependency_failed": "dependency failed",
    "workflow.policy_set": "policy set",
    "workflow.usage_recorded": "usage recorded",
}


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _child_environment() -> dict[str, str]:
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("LOGPLANE_")}
    source = str(Path(__file__).resolve().parents[1])  # the directory containing logplane/
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = source + (os.pathsep + existing if existing else "")
    environment["PYTHONUNBUFFERED"] = "1"
    return environment


class Harness:
    def __init__(self, workdir: Path, *, out: TextIO, pace: float = 1.0,
                 interactive: bool = False, color: bool = False, read_pauses: bool = False,
                 lease_seconds: Optional[float] = None):
        if pace <= 0:
            raise ValueError("pace must be positive")
        if lease_seconds is not None and lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.workdir = workdir
        self.out = out
        self.pace = pace
        self.interactive = interactive
        self.color = color
        self.read_pauses = read_pauses
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.lease_seconds = lease_seconds if lease_seconds is not None else max(1.5, 4.0 * pace)
        self.heartbeat_seconds = self.lease_seconds / 4
        self.config_path = self._write_config()
        self.logs = workdir / "logs"
        self.logs.mkdir(exist_ok=True)
        self.processes: dict[str, subprocess.Popen] = {}
        self._print_lock = threading.Lock()
        self._tail_stop = threading.Event()
        self._tail_thread: Optional[threading.Thread] = None
        self._tail_observer = None
        self.environment = _child_environment()

    # -- output -----------------------------------------------------------
    def _style(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.color else text

    def emit(self, text: str = "") -> None:
        with self._print_lock:
            print(text, file=self.out, flush=True)

    def title(self, text: str) -> None:
        self.emit()
        self.emit(self._style("1", f"━━ {text} ━━"))

    def say(self, text: str) -> None:
        self.emit()
        width = max(40, shutil.get_terminal_size((100, 24)).columns - 1)
        wrapped = textwrap.fill(text, width=width, initial_indent="▸ ", subsequent_indent="  ")
        for line in wrapped.splitlines():
            self.emit(self._style("1;36", line))
        self.reading_pause(text)

    def reading_pause(self, text: str) -> None:
        """With --read-pauses, hold long enough to read the narration (~4 words/s)."""
        if self.read_pauses:
            time.sleep(min(8.0, max(1.5, len(text.split()) / 4.0)))

    def command(self, argv: list[str]) -> None:
        self.emit(self._style("33", "$ " + " ".join(shlex.quote(part) for part in argv)))

    def block(self, text: str) -> None:
        for line in text.rstrip("\n").splitlines():
            self.emit("  " + line)

    def pause(self, seconds: float) -> None:
        time.sleep(seconds * self.pace)

    # -- setup ------------------------------------------------------------
    def _write_config(self) -> Path:
        from logplane.local_config import initialize_local_config

        path = initialize_local_config(self.workdir)
        value = json.loads(path.read_text(encoding="utf-8"))
        value["bus"]["port"] = self.port
        value["worker_lease_seconds"] = self.lease_seconds
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path

    def _spawn(self, key: str, argv: list[str]) -> subprocess.Popen:
        log = (self.logs / f"{key}.log").open("a", encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, *argv], cwd=self.workdir, env=self.environment,
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
        log.close()
        self.processes[key] = process
        return process

    def start_bus(self) -> None:
        from logplane.observer import ObserverClient

        self._spawn("bus", ["-m", "logplane.cli", "serve", "--config", str(self.config_path)])
        observer = ObserverClient(self.url)
        try:
            self.wait_until(lambda: _healthy(observer), 20, "the bus to start")
        finally:
            observer.close()
        self._start_tail()

    def start_pm(self) -> None:
        self._spawn("pm", ["-m", "logplane.cli", "pm", "--config", str(self.config_path)])

    def start_worker(self, name: str, capabilities: Iterable[str] = (), capacity: int = 1) -> None:
        argv = ["-m", "logplane.demo", "_worker", "--url", self.url, "--name", name,
                "--workdir", str(self.workdir), "--capacity", str(capacity),
                "--heartbeat", str(self.heartbeat_seconds)]
        for capability in capabilities:
            argv += ["--capability", capability]
        # A registration from an earlier process with the same name must not count.
        mark = self.last_event_id()
        self._spawn(name, argv)
        self.wait_until(lambda: self.find_event("agent.registered", actor=name, after=mark),
                        20, f"worker {name} to register")

    def kill(self, key: str) -> None:
        process = self.processes.pop(key)
        if process.poll() is None:
            os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=10)

    def stop_all(self) -> None:
        self._tail_stop.set()
        if self._tail_observer is not None:
            # Drop the open event stream first so the bus can shut down promptly.
            with contextlib.suppress(Exception):
                self._tail_observer.close()
        for key in ["pm", *[k for k in self.processes if k not in {"pm", "bus"}], "bus"]:
            process = self.processes.pop(key, None)
            if process is None or process.poll() is not None:
                continue
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    # -- reading state ----------------------------------------------------
    def events(self, topics: Optional[list[str]] = None, after: int = 0) -> list[dict]:
        from logplane.observer import ObserverClient

        observer = ObserverClient(self.url)
        try:
            return list(observer.query_all(topics=topics, after_id=after))
        finally:
            observer.close()

    def last_event_id(self) -> int:
        from logplane.observer import ObserverClient

        observer = ObserverClient(self.url)
        try:
            return int(observer.health().get("last_event_id", 0))
        finally:
            observer.close()

    def find_event(self, topic: str, *, task_id: Optional[int] = None,
                   actor: Optional[str] = None, after: int = 0) -> Optional[dict]:
        for event in self.events([topic], after=after):
            if task_id is not None and event.get("payload", {}).get("task_id") != task_id:
                continue
            if actor is not None and event.get("actor") != actor:
                continue
            return event
        return None

    def wait_until(self, predicate: Callable[[], Any], timeout: float, what: str) -> Any:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = predicate()
            except Exception:  # the bus may still be starting
                value = None
            if value:
                return value
            time.sleep(0.2)
        raise DemoError(f"timed out waiting for {what}; process logs are in {self.logs}")

    def wait_for(self, topic: str, *, task_id: Optional[int] = None,
                 actor: Optional[str] = None, timeout: float = 60) -> dict:
        label = f"{topic}" + (f" for task {task_id}" if task_id else "")
        return self.wait_until(lambda: self.find_event(topic, task_id=task_id, actor=actor),
                               timeout, label)

    def workflow_state(self, workflow: str) -> dict:
        code, output = self.view(["workflow", workflow, "--json"], show=False)
        if code != 0:
            raise DemoError(f"could not read workflow {workflow}: {output}")
        return json.loads(output)

    def task_state(self, task_id: int) -> dict:
        code, output = self.view(["task", str(task_id), "--json"], show=False)
        if code != 0:
            raise DemoError(f"could not read task {task_id}: {output}")
        return json.loads(output)

    def wait_status(self, task_id: int, status: str, timeout: float = 90) -> dict:
        def reached():
            state = self.task_state(task_id)
            return state if state.get("status") == status else None

        return self.wait_until(reached, timeout, f"task {task_id} to be {status}")

    # -- interaction ------------------------------------------------------
    def confirm_external_send(self, prompt: str, *, assume_yes: bool = False) -> bool:
        """Paid/external work requires consent, unlike automatic stand-in demos."""
        if assume_yes:
            self.emit(f"  {prompt}: yes (explicit --yes)")
            return True
        if not self.interactive:
            raise DemoError("nothing was sent: external model calls need confirmation. "
                            "Run in a terminal, or pass --yes to explicitly approve them.")
        return self.ask(prompt, default="no", choices=("yes", "no")) == "yes"

    def ask(self, prompt: str, default: str, choices: Iterable[str] = ()) -> str:
        choices = tuple(choices)
        if not self.interactive:
            self.emit(f"  {prompt}: {default} (automatic; run in a terminal without --yes to answer)")
            return default
        with self._print_lock:
            while True:
                try:
                    answer = input(f"  {prompt} [{default}]: ").strip()
                except EOFError:
                    answer = ""
                if not answer:
                    return default
                if not choices:
                    return answer
                matched = normalize_choice(answer, choices)
                if matched is not None:
                    return matched
                print(f"  Please answer {' or '.join(choices)}.", file=self.out, flush=True)

    def recap(self, points: list[str]) -> None:
        self.emit()
        self.emit(self._style("1;36", "▸ What just happened"))
        width = max(40, shutil.get_terminal_size((100, 24)).columns - 1)
        for point in points:
            for line in textwrap.fill(point, width=width, initial_indent="  • ",
                                      subsequent_indent="    ").splitlines():
                self.emit(line)
        self.reading_pause(" ".join(points))

    # -- running the real CLI ---------------------------------------------
    def view(self, args: list[str], *, show: bool = True, shown_as: Optional[list[str]] = None):
        """Run a read-only CLI view in-process against this demo's bus."""
        from logplane import cli as logplane_cli

        if show:
            self.command(["logplane", *(shown_as or args)])
        stdout, stderr = io.StringIO(), io.StringIO()
        code = logplane_cli.main([*args, "--url", self.url], stdout=stdout, stderr=stderr)
        output = stdout.getvalue() if code == 0 else stderr.getvalue()
        if show:
            self.block(output)
        return code, output

    def cli(self, args: list[str], *, shown_as: Optional[list[str]] = None,
            show_output: bool = True, quiet: bool = False) -> str:
        """Run a writing CLI command as a separate process, as a user would."""
        if not quiet:
            self.command(["logplane", *(shown_as or args)])
        completed = subprocess.run(
            [sys.executable, "-m", "logplane.cli", *args, "--config", str(self.config_path)],
            cwd=self.workdir, env=self.environment, capture_output=True, text=True, timeout=60,
        )
        if completed.returncode != 0:
            raise DemoError(f"logplane {' '.join(args)} failed: {completed.stderr.strip()}")
        if show_output and not quiet:
            self.block(completed.stdout)
        return completed.stdout

    def submit(self, title: str, *, depends_on: Iterable[int] = (),
               capability: Optional[str] = None, context: Optional[dict] = None,
               max_retries: int = 2, correlation_id: Optional[str] = None,
               quiet: bool = False) -> tuple[int, str]:
        shown = ["submit", title]
        if correlation_id:
            shown += ["--correlation-id", correlation_id]
        for upstream in depends_on:
            shown += ["--depends-on", str(upstream)]
        if capability:
            shown += ["--capability", capability]
        shown += ["--max-retries", str(max_retries)]
        args = [*shown, "--context", json.dumps(context or {}), "--json"]
        event = json.loads(self.cli(args, shown_as=shown, show_output=False, quiet=quiet))
        task_id = event["payload"]["task_id"]
        if not quiet:
            self.emit(f"  Submitted task {task_id}")
        return task_id, event["correlation_id"]

    # -- live event tail --------------------------------------------------
    def _start_tail(self) -> None:
        self._tail_thread = threading.Thread(target=self._tail, daemon=True)
        self._tail_thread.start()

    def _tail(self) -> None:
        from logplane.observer import ObserverClient

        observer = ObserverClient(self.url)
        self._tail_observer = observer
        try:
            for event in observer.subscribe(topics=list(SHOWN_TOPICS)):
                if self._tail_stop.is_set():
                    return
                self.emit(self._style("2", "    " + describe_event(event)))
        except Exception:
            return


def _healthy(observer) -> bool:
    return bool(observer.health().get("ok"))


def describe_event(event: Mapping[str, Any]) -> str:
    payload = event.get("payload") or {}
    topic = str(event.get("topic"))
    label = SHOWN_TOPICS.get(topic, topic)
    task = payload.get("task_id")
    actor = event.get("actor")
    detail = ""
    if topic == "agent.registered":
        detail = f"worker {actor}"
    elif topic == "task.created":
        detail = f"task {task}: {payload.get('title')}"
        if payload.get("depends_on"):
            detail += f" (after {', '.join(map(str, payload['depends_on']))})"
    elif topic == "task.assigned":
        detail = f"task {task} → {payload.get('assignee')} (attempt {payload.get('attempt')})"
    elif topic == "task.assignment_expired":
        detail = f"task {task}: {payload.get('reason', 'lease expired')}"
    elif topic in {"task.blocked", "decision.needed"}:
        detail = f"task {task}"
    elif topic == "workflow.usage_recorded":
        cost = payload.get("cost_usd")
        tokens = payload.get("tokens")
        money = (f"${cost:.3f}" if isinstance(cost, (int, float)) and cost < 0.1
                 else f"${cost:.2f}" if isinstance(cost, (int, float)) else "cost unknown")
        detail = (f"task {task}: {money}"
                  + (f", {tokens:,} tokens" if isinstance(tokens, int) else ""))
    elif topic == "workflow.policy_set":
        limit = payload.get("max_total_cost_usd")
        detail = (f"cost cap ${limit:.2f} by {actor}" if isinstance(limit, (int, float))
                  else f"workflow defaults by {actor}")
    elif topic == "decision.made":
        detail = f"task {task}: {json.dumps(payload.get('decision'))} by {actor}"
    else:
        detail = f"task {task} by {actor}" if task is not None else f"by {actor}"
    return f"#{event.get('id'):<4} {label:<19} {detail}"


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Scenario:
    name: str
    summary: str
    run: Callable[[Harness], None]


def usage(input_tokens: int, output_tokens: int, cost_usd: float) -> dict:
    """Stand-in model usage for one call, reported through normal telemetry."""
    return {"input_tokens": input_tokens, "output_tokens": output_tokens,
            "cost_usd": cost_usd}


def scenario_recovery(h: Harness) -> None:
    h.title("Kill anything, lose nothing")
    h.say("A three-step workflow runs on one worker, alice. Part-way through step 2 "
          "we SIGKILL both alice and the coordinator (the PM), then start replacements.")
    h.start_bus()
    h.start_pm()
    h.start_worker("alice")
    t1, workflow = h.submit("Fetch market data", context={"work_seconds": 1 * h.pace})
    t2, _ = h.submit("Analyse price feeds", depends_on=[t1], context={
        "work_seconds": 30 * h.pace, "retry_work_seconds": 1 * h.pace,
        "model_usage": usage(12000, 1500, 0.09)})
    t3, _ = h.submit("Write summary", depends_on=[t2], context={
        "work_seconds": 1 * h.pace, "model_usage": usage(4000, 900, 0.03)})
    h.wait_for("task.started", task_id=t2, actor="alice")
    h.pause(1.5)
    h.kill("alice")
    h.kill("pm")
    h.say("alice was part-way through task 2, so we just pulled the plug on alice AND the PM "
          "(kill -9).")
    count = h.last_event_id()
    h.say(f"Nothing is coordinating now. All state is in the log: {count} immutable events. "
          "The bus still answers questions from it:")
    h.view(["task", str(t2)])
    h.say("The log hasn't been told that alice died, and nothing guesses. Her lease simply "
          "stops being renewed.")
    h.say("Start a fresh PM and a new worker, bob. The PM rebuilds state by replaying the log. "
          "Once alice's lease runs out, it expires her attempt and reassigns the task to bob.")
    h.start_pm()
    h.start_worker("bob")
    h.wait_status(t3, "completed")
    h.pause(0.5)
    h.say("Finished. Task 2 shows both attempts, and the workflow completed in order:")
    h.view(["task", str(t2)])
    h.view(["workflow", workflow, "--mermaid"])
    h.recap([
        "alice's attempt was expired by lease, not guessed at; it's an event in the log.",
        "Attempt 2 went to bob. If alice came back, her late result would be rejected as stale.",
        "Task 3 waited for task 2 the whole time; nothing ran out of order.",
    ])


def scenario_approval(h: Harness) -> None:
    h.title("An agent asks before it acts")
    h.say("A support agent handles ticket #4411. It decides a refund is due, but issuing "
          "one is irreversible, so it asks a human before calling the refund tool. "
          "Drafting the reply carries on meanwhile.")
    h.start_bus()
    h.start_pm()
    h.start_worker("support-agent", capacity=2)
    t1, workflow = h.submit("Read ticket #4411", context={
        "work_seconds": 1 * h.pace, "model_usage": usage(2400, 300, 0.012)})
    t2, _ = h.submit("Resolve ticket #4411", depends_on=[t1], context={
        "stage": "approval", "work_seconds": 1 * h.pace,
        "tool": "issue_refund", "arguments": {"order": 4411, "amount_usd": "120.00"},
        "call": "issue_refund(order=4411, amount_usd=120.00)",
        "model_usage": usage(3100, 220, 0.014)})
    t3, _ = h.submit("Draft the reply", depends_on=[t1], context={
        "work_seconds": 2 * h.pace, "model_usage": usage(2800, 450, 0.016)})
    t4, _ = h.submit("Send the reply", depends_on=[t2, t3], context={
        "stage": "reply", "work_seconds": 1 * h.pace})
    needed = h.wait_for("decision.needed", task_id=t2)
    h.wait_status(t3, "completed")
    h.say("The agent stopped before the tool call and asked. The draft finished anyway; "
          "sending waits, and says why:")
    h.view(["decisions"])
    h.view(["explain", str(t4)])
    answer = h.ask("Approve the refund? (approve/deny)", default="approve",
                   choices=APPROVAL_CHOICES)
    h.say("The answer is one command, recorded as an event like everything else:")
    h.cli(["decide", str(t2), "--decision-event", str(needed["id"]), "--value", json.dumps(answer)])
    h.wait_status(t4, "completed")
    h.pause(0.5)
    h.say("A new attempt of task 2 acted on the answer, and the reply went out:")
    h.view(["task", str(t2)])
    h.view(["workflow", workflow])
    ledger = read_inbox(h.workdir / REFUND_LEDGER)
    h.block(f"{REFUND_LEDGER}: {len(ledger)} refund(s) issued")
    if answer == "deny":
        first = ("The agent never reached the refund tool: a human said no, and that answer is "
                 "on record with who gave it and which attempt used it.")
    else:
        first = ("The agent could not reach the refund tool until a human said yes; the approval "
                 "is on record with who gave it and which attempt used it.")
    h.recap([
        first,
        "The refund carried assignment.effect_id as its idempotency key, and the payments "
        "system rejects a key it has seen, so a retried attempt cannot refund twice.",
        "Waiting for a human didn't stall the draft, and it used none of the task's retries.",
    ])


def scenario_budget(h: Harness) -> None:
    h.title("A cost cap stops new work at the limit")
    h.say("An agent fleet has ten papers to summarise, at about $0.40 of model usage each. "
          "Left alone that is $4. The workflow gets a $2.00 cap before any work starts.")
    h.start_bus()
    h.start_pm()
    work = {"work_seconds": 1.5 * h.pace, "model_usage": usage(9000, 1300, 0.4)}
    first, workflow = h.submit("Summarise paper 1", context=work, quiet=True)
    for number in range(2, 11):
        h.submit(f"Summarise paper {number}", context=work, correlation_id=workflow, quiet=True)
    h.command(["logplane", "submit", "Summarise paper N", "--correlation-id", workflow])
    h.emit(f"  Submitted tasks {first}–{first + 9} (one workflow)")
    h.cli(["policy", workflow, "--max-cost", "2", "--reserve-cost-per-assignment", "0.5",
           "--reason", "nightly research budget"])
    h.say("Each assignment reserves $0.50 until its cost is reported, so work is admitted "
          "against what running tasks may still spend. This gates new work; it is not a "
          "billing cutoff for a call already running. Start the workers:")
    h.start_worker("worker-1")
    h.start_worker("worker-2")

    def capped():
        state = h.workflow_state(workflow)
        budget = state["budget"]
        busy = any(task["status"] in {"assigned", "started"} for task in state["tasks"])
        return state if budget.get("blocked") and not busy else None

    state = h.wait_until(capped, 120, "the cost cap to stop new work")
    done = sum(task["status"] == "completed" for task in state["tasks"])
    spent = state["budget"]["cost_usd"]
    waiting = next(task["task_id"] for task in state["tasks"] if task["status"] == "open")
    h.say(f"Work stopped after {done} summaries at ${spent:.2f}: one more would reserve "
          "$0.50 and take the committed total past the $2.00 cap. Nothing was killed; running "
          "tasks finished and new ones wait. The views say why:")
    h.view(["workflow", workflow])
    h.view(["explain", str(waiting)])
    answer = h.ask("Raise the cap to $5.00? (yes/no)", default="yes", choices=("yes", "no"))
    if answer == "no":
        h.say("The remaining papers stay queued. Raising the cap later picks up where it "
              "left off.")
        h.recap([
            "The cap stopped new work without killing in-flight work.",
            "Queued tasks wait with an explanation instead of failing.",
        ])
        return
    h.say("Raising the cap is a new policy event, so the approval of extra spend is on record:")
    h.cli(["policy", workflow, "--max-cost", "5", "--reserve-cost-per-assignment", "0.5",
           "--reason", "approved: finish the batch tonight"])
    h.wait_until(lambda: all(task["status"] == "completed"
                             for task in h.workflow_state(workflow)["tasks"]),
                 120, "the remaining summaries")
    h.pause(0.5)
    final = h.workflow_state(workflow)["budget"]
    h.say(f"All ten are done: {final['tokens']:,} tokens and ${final['cost_usd']:.2f}, "
          "recorded per model call and attempt. One finished task, for example:")
    h.view(["task", str(waiting)])
    h.recap([
        "The cap stopped new work at the limit; in-flight tasks finished normally.",
        "Reservations counted running work against the cap before its cost was reported.",
        "Both the cap and the raise are events: who allowed more spend, when, and why.",
    ])


def scenario_audit_triage(h: Harness) -> None:
    h.title("Audit triage: parallel reviewers, a human severity call, idempotent submission")
    h.say("A fictional protocol with three contracts. A lead agent scopes the audit, two reviewer "
          "agents split the contracts, duplicate findings are merged, a human makes the severity "
          "call, and the lead writes the report and submits the findings.")
    h.start_bus()
    h.start_pm()
    h.start_worker("lead", capabilities=["lead"])
    h.start_worker("reviewer-1", capabilities=["review"])
    h.start_worker("reviewer-2", capabilities=["review"])
    contracts = list(REVIEW_FINDINGS)
    scope, workflow = h.submit("Scope the Lumen audit", capability="lead", context={
        "stage": "scope", "contracts": contracts, "work_seconds": 1 * h.pace,
        "model_usage": usage(6000, 400, 0.03)})
    reviews = []
    for contract in contracts:
        task, _ = h.submit(f"Review {contract}", depends_on=[scope], capability="review", context={
            "stage": "review", "contract": contract,
            "work_seconds": 6 * h.pace, "retry_work_seconds": 2 * h.pace,
            "model_usage": usage(28000, 2600, 0.21)})
        reviews.append(task)
    dedupe, _ = h.submit("Merge duplicate findings", depends_on=reviews, capability="lead",
                         context={"stage": "dedupe", "work_seconds": 1 * h.pace,
                                  "model_usage": usage(9000, 1100, 0.07)})
    triage, _ = h.submit("Triage severity", depends_on=[dedupe], capability="lead",
                         context={"stage": "triage", "work_seconds": 0.5 * h.pace,
                                  "model_usage": usage(5000, 600, 0.04)})
    report, _ = h.submit("Write the report", depends_on=[triage], capability="lead",
                         context={"stage": "report", "work_seconds": 1 * h.pace,
                                  "model_usage": usage(7000, 2400, 0.08)})
    submit, _ = h.submit("Submit findings to the contest", depends_on=[report],
                         capability="lead",
                         context={"stage": "submit", "pause_seconds": 4 * h.pace})

    started = h.wait_for("task.started", actor="reviewer-1")
    h.pause(1.5)
    victim = started["payload"]["task_id"]
    h.kill("reviewer-1")
    h.say(f"reviewer-1 was part-way through task {victim} when it crashed (kill -9).")
    h.say("reviewer-2 is still working. When reviewer-1's lease expires, its review is "
          "reassigned to reviewer-2.")
    needed = h.wait_for("decision.needed", task_id=triage, timeout=120)
    h.say("All three reviews are in, and the duplicates were merged. The lead agent wants a "
          "human to make one severity call:")
    h.view(["decisions"])
    answer = h.ask("Severity (High/Medium)", default="High", choices=SEVERITIES)
    h.cli(["decide", str(triage), "--decision-event", str(needed["id"]), "--value", json.dumps(answer)])

    inbox = h.workdir / INBOX_FILE
    h.wait_until(lambda: read_inbox(inbox), 90, "the first submission")
    h.kill("lead")
    h.say("The lead submitted its first finding to the contest platform, then crashed before "
          "reporting back (kill -9).")
    h.block(f"{INBOX_FILE}: {len(read_inbox(inbox))} submission(s) so far")
    h.say("A replacement lead starts. Attempt 2 of the submit task has the same effect keys, "
          "so it skips what attempt 1 already submitted.")
    h.start_worker("lead", capabilities=["lead"])
    h.wait_status(submit, "completed", timeout=120)
    h.pause(0.5)
    h.view(["task", str(submit)])
    entries = read_inbox(inbox)
    h.say(f"The contest platform received {len(entries)} submissions, one per finding:")
    for entry in entries:
        h.block(f"{entry['finding']} [{entry['severity']}] attempt {entry['attempt']}: {entry['title']}")
    h.say("The report the lead wrote:")
    h.block((h.workdir / REPORT_FILE).read_text(encoding="utf-8"))
    h.view(["workflow", workflow, "--mermaid"])
    budget = h.workflow_state(workflow)["budget"]
    h.say(f"Model usage for the whole audit: {budget['tokens']:,} tokens and "
          f"${budget['cost_usd']:.2f}, recorded per model call and attempt. The crashed "
          "reviewer's call never finished, so it has no usage to record; nothing is guessed.")
    h.recap([
        "Two reviewers shared three contracts, and the review on a crashed reviewer was "
        "reassigned rather than lost.",
        "Four raw findings became three. The contested one waited for a human, and the "
        "answer is on record.",
        "Each finding reached the platform once: the submitter crashed mid-batch, and the "
        "retry's repeated keys were rejected.",
    ])


SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario
    for scenario in (
        Scenario("recovery", "kill the worker and the coordinator mid-task; nothing is lost",
                 scenario_recovery),
        Scenario("approval", "an agent asks before an irreversible tool call; other work "
                 "continues", scenario_approval),
        Scenario("budget", "a cost cap stops new work; raise it and work resumes",
                 scenario_budget),
        Scenario("audit-triage", "parallel reviewers, a human severity call, idempotent "
                 "submission", scenario_audit_triage),
    )
}


def run(scenario_name: Optional[str], *, out: TextIO = sys.stdout, pace: float = 1.0,
        assume_yes: bool = False, directory: Optional[str] = None,
        keep_running: Optional[bool] = None, stdin: TextIO = sys.stdin,
        read_pauses: bool = False) -> int:
    if scenario_name is None or scenario_name not in SCENARIOS:
        if scenario_name is not None:
            print(f"logplane: unknown demo scenario: {scenario_name}", file=out)
        print("Scenarios:", file=out)
        for scenario in SCENARIOS.values():
            print(f"  {scenario.name:<14} {scenario.summary}", file=out)
        print("\nRun one with: logplane demo recovery", file=out)
        return 0 if scenario_name is None else 2

    def play(harness: Harness) -> None:
        harness.emit("Real processes, events, and crashes; scripted stand-in agents, so their")
        harness.emit("outputs, token counts, and costs are simulated.")
        SCENARIOS[scenario_name].run(harness)

    return run_session(
        play, prefix=f"logplane-demo-{scenario_name}-", label="logplane demo",
        out=out, pace=pace, assume_yes=assume_yes, directory=directory,
        keep_running=keep_running, stdin=stdin, read_pauses=read_pauses,
    )


def run_session(scenario: Callable[[Harness], None], *, prefix: str, label: str,
                out: TextIO = sys.stdout, pace: float = 1.0, assume_yes: bool = False,
                directory: Optional[str] = None, keep_running: Optional[bool] = None,
                stdin: TextIO = sys.stdin, read_pauses: bool = False,
                lease_seconds: Optional[float] = None) -> int:
    """Shared lifecycle for scripted demos and repository-pinned real examples.

    Automatic demo answers are not consent to send data externally: real
    scenarios must call ``confirm_external_send`` before starting paid work.
    """
    if directory:
        workdir = Path(directory).expanduser().resolve()
        if workdir.exists() and any(workdir.iterdir()):
            print(f"{label}: directory must be new or empty: {workdir}", file=out)
            return 2
        workdir.mkdir(parents=True, exist_ok=True)
    else:
        workdir = Path(tempfile.mkdtemp(prefix=prefix)).resolve()
    tty = hasattr(stdin, "isatty") and stdin.isatty() and hasattr(out, "isatty") and out.isatty()
    interactive = tty and not assume_yes
    color = hasattr(out, "isatty") and out.isatty() and "NO_COLOR" not in os.environ
    harness = Harness(workdir, out=out, pace=pace, interactive=interactive, color=color,
                      read_pauses=read_pauses, lease_seconds=lease_seconds)
    harness.emit(f"Directory: {workdir}")
    harness.emit(f"Bus: {harness.url} (lease {harness.lease_seconds:g}s; separate from any bus "
                 "you already run)")
    code = 0
    try:
        scenario(harness)
        if keep_running is None:
            keep_running = interactive
        if keep_running:
            harness.say(f"The bus is still running at {harness.url}. Explore it from another "
                        "terminal, for example:")
            harness.block(f"logplane --url {harness.url} workflows\n"
                          f"logplane --url {harness.url} tasks")
            harness._tail_stop.set()
            with contextlib.suppress(EOFError):
                input("  Press Enter to stop the demo. ")
    except DemoError as exc:
        harness.emit(f"\n{label}: {exc}")
        code = 1
    except KeyboardInterrupt:
        code = 130
    finally:
        harness.stop_all()
    harness.emit()
    harness.emit(f"History is kept in {workdir}. To reopen it later:")
    harness.emit(f"  logplane serve --config {harness.config_path}")
    return code


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_worker":
        raise SystemExit(_worker_main(sys.argv[2:]))
    raise SystemExit(run(sys.argv[1] if len(sys.argv) > 1 else None))
