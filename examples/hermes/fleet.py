"""Shared plumbing for the Hermes examples that run a whole workflow.

Builds on ``logplane.demo.Harness``, the helper behind ``logplane demo``: a
throwaway bus and coordinator on a free port, real worker processes, a live
event tail, and narration. The harness is a CLI implementation detail rather
than a public API, so these examples pin to the repository they ship with.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Callable, TextIO

from logplane.demo import DemoError, Harness, run_session

REPO_ROOT = Path(__file__).resolve().parents[2]


def add_hermes_arguments(parser: argparse.ArgumentParser, *, default_pace: float) -> None:
    parser.add_argument("--provider", required=True, help="Hermes provider, made explicit")
    parser.add_argument("--model", required=True, help="Hermes model, made explicit")
    parser.add_argument("--hermes-command", default="hermes", help="Hermes executable")
    parser.add_argument("--reasoning", help="optional Hermes reasoning effort override")
    parser.add_argument("--timeout", type=float, default=600.0,
                        help="hard wall-clock limit per Hermes invocation (seconds)")
    parser.add_argument("--pace", type=float, default=default_pace, help="narration pace")
    parser.add_argument("--lease", type=float, default=30.0,
                        help="worker lease in seconds; a worker silent this long loses its task")
    parser.add_argument("--yes", action="store_true", help="answer every question with its default")
    parser.add_argument("--dir", help="new or empty directory to keep the history in")
    parser.add_argument("--keep-running", action=argparse.BooleanOptionalAction, default=None)


def run_example(args: argparse.Namespace, scenario: Callable[[Harness, argparse.Namespace], None],
                *, prefix: str, out: TextIO = sys.stdout, stdin: TextIO = sys.stdin) -> int:
    """Run one scenario with the same lifecycle as ``logplane demo``."""
    def play(h: Harness) -> None:
        # Child processes need the repository on their path for the examples package.
        h.environment["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + h.environment["PYTHONPATH"]
        h.environment["LOGPLANE_HEARTBEAT_SECONDS"] = str(h.heartbeat_seconds)
        h.emit(f"Hermes: {args.provider} / {args.model}, safe mode, clarify toolset only. "
               "Its model calls are real and paid.")
        if sys.platform == "darwin":
            h.emit("This takes a while. If the Mac sleeps, every process pauses and leases expire; "
                   "run under `caffeinate -i` to keep it awake.")
        scenario(h, args)

    return run_session(
        play, prefix=f"logplane-{prefix}-", label=prefix, out=out, stdin=stdin,
        directory=args.dir, pace=args.pace, assume_yes=args.yes,
        keep_running=args.keep_running, lease_seconds=args.lease,
    )


def start_hermes_worker(h: Harness, args: argparse.Namespace, name: str,
                        capability: str = "hermes") -> None:
    work = h.workdir / f"{name}-work"
    work.mkdir(exist_ok=True)
    argv = ["-m", "examples.hermes.run_worker", "--bus-url", h.url, "--name", name,
            "--working-directory", str(work), "--provider", args.provider,
            "--model", args.model, "--hermes-command", args.hermes_command,
            "--timeout", str(args.timeout), "--capability", capability]
    if args.reasoning:
        argv += ["--reasoning", args.reasoning]
    _start(h, name, argv)


def start_python_worker(h: Harness, module: str, name: str) -> None:
    """Start a worker defined by ``python -m <module> _worker`` in this package."""
    argv = ["-m", module, "_worker", "--url", h.url, "--name", name,
            "--workdir", str(h.workdir), "--heartbeat", str(h.heartbeat_seconds)]
    _start(h, name, argv)


def _start(h: Harness, name: str, argv: list[str]) -> None:
    mark = h.last_event_id()
    h._spawn(name, argv)
    h.wait_until(lambda: h.find_event("agent.registered", actor=name, after=mark),
                 20, f"worker {name} to register")


def wait_done(h: Harness, task_id: int, timeout: float, *, log: str = "logs") -> dict:
    """Wait for a task to finish; a failure stops the example with a pointer."""
    terminal = {"completed", "failed", "cancelled", "dependency_failed"}

    def finished():
        state = h.task_state(task_id)
        return state if state["status"] in terminal else None

    state = h.wait_until(finished, timeout, f"task {task_id} to finish")
    if state["status"] != "completed":
        hint = ""
        expired = [e for e in h.events(["task.assignment_expired"])
                   if e["payload"].get("task_id") == task_id]
        if len(expired) >= 2:
            hint = (" Its leases expired repeatedly: if the machine slept or was overloaded, "
                    "every process paused together; rerun under caffeinate -i or with a longer "
                    "--lease.")
        raise DemoError(f"task {task_id} ended {state['status']}; see {log}/ and run: "
                        f"logplane --url {h.url} explain {task_id}.{hint}")
    return state


def run_python_worker(argv: list[str], name: str, capability: str,
                      make_executor: Callable[[Path], object]) -> int:
    from logplane.client import BusClient
    from logplane.runtime import WorkerRuntime

    parser = argparse.ArgumentParser(prog=name)
    parser.add_argument("--url", required=True)
    parser.add_argument("--name", default=name)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--heartbeat", type=float, default=1.0)
    args = parser.parse_args(argv)
    runtime = WorkerRuntime(
        BusClient(args.url, actor=args.name), name=args.name,
        executor=make_executor(Path(args.workdir)), capabilities=[capability],
        heartbeat_seconds=args.heartbeat,
    )
    print(f"[{args.name}] registered, waiting for work", flush=True)
    try:
        runtime.run()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", flush=True)
    return 0
