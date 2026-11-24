"""One contest worker: a model, a stage, and the runner that drives it.

Runners:
  claude  Claude Code with your Claude login, read-only tools in the workspace
  codex   Codex with your ChatGPT login, read-only sandbox in the workspace
  hermes  Hermes Agent with a provider such as nous; no tools, so the files a
          task needs are given inline
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import uuid
from pathlib import Path

from examples.contest_hunt.schemas import REQUIRED_KEYS, SCHEMAS
from logplane.client import BusClient
from logplane.executors import Completed, PermanentFailure
from logplane.runtime import WorkerRuntime
from logplane.telemetry import BusTelemetrySink, ProducerIdentity

RUNNERS = ("claude", "codex", "hermes")
STAGES = tuple(SCHEMAS)
MAX_PROMPT_BYTES = 384 * 1024
MAX_INLINE_FILE_BYTES = 160 * 1024
INLINE_NOTE = (
    "You have no tools. The files this task refers to are included below, after the "
    "instructions; you cannot read any others. Where the instructions say to read a file "
    "that is not included, work from what is included.\n\n"
)


class BriefingExecutor:
    """Expand a task's brief file into its goal, and check the result's shape.

    Tasks carry only a short title and the path of a brief in the workspace,
    which keeps the coordination log small. Runners without tools also get the
    files listed in ``inline_files``.
    """

    def __init__(self, delegate, *, workspace: Path, stage: str, inline: bool):
        # Resolved, so the containment check below compares like with like even
        # when the workspace path runs through a symlink (on macOS, /var is
        # /private/var).
        self.delegate, self.stage, self.inline = delegate, stage, inline
        self.workspace = Path(workspace).resolve()
        self.last_report = None

    def _read(self, relative: str, limit: int) -> str:
        path = (self.workspace / relative).resolve()
        if self.workspace != path and self.workspace not in path.parents:
            raise ValueError(f"{relative} is outside the workspace")
        data = path.read_bytes()[:limit]
        return data.decode("utf-8", errors="replace")

    def execute(self, assignment):
        context = dict(assignment.context)
        brief = context.pop("brief", None)
        inline_files = context.pop("inline_files", [])
        if not isinstance(brief, str):
            return PermanentFailure("contest_no_brief", "Task has no brief to follow")
        try:
            goal = self._read(brief, MAX_PROMPT_BYTES)
            if self.inline:
                parts = [INLINE_NOTE + goal]
                for relative in inline_files:
                    parts.append(f"\n\n===== {relative} =====\n" + self._read(relative, MAX_INLINE_FILE_BYTES))
                goal = "".join(parts)
        except (OSError, ValueError) as exc:
            return PermanentFailure("contest_brief_unreadable", f"Could not read the brief: {exc}")
        outcome = self.delegate.execute(dataclasses.replace(assignment, goal=goal, context=context))
        self.last_report = getattr(self.delegate, "last_report", None)
        if isinstance(outcome, Completed):
            missing = [key for key in REQUIRED_KEYS[self.stage] if key not in outcome.result]
            if missing:
                return PermanentFailure("contest_bad_result",
                                        f"{self.stage} result is missing {', '.join(missing)}")
        return outcome

    def cancel(self, assignment_id):
        self.delegate.cancel(assignment_id)

    def close(self):
        self.delegate.close()


def build_executor(args, telemetry):
    schema = SCHEMAS[args.stage]
    workspace = args.workspace.resolve()
    if args.runner == "claude":
        from examples.claude_code.claude_code_executor import ClaudeCodeExecutor
        return ClaudeCodeExecutor(
            auth="login", working_directory=workspace, command=(args.command or "claude",),
            model=args.model, effort=args.effort, read_only_tools=True, timeout=args.timeout,
            max_turns=args.max_turns, max_budget_usd=args.max_budget_usd,
            telemetry_sink=telemetry, result_schema=schema, max_prompt_bytes=MAX_PROMPT_BYTES)
    if args.runner == "codex":
        from examples.codex.codex_executor import CodexExecutor
        return CodexExecutor(
            working_directory=workspace, command=(args.command or "codex",), model=args.model,
            effort=args.effort, timeout=args.timeout, result_schema=schema,
            max_prompt_bytes=MAX_PROMPT_BYTES, telemetry_sink=telemetry, daybreak=args.daybreak)
    from examples.hermes.hermes_executor import HermesExecutor
    scratch = workspace.parent / "hermes-scratch"
    scratch.mkdir(exist_ok=True)
    return HermesExecutor(
        working_directory=scratch, model=args.model, provider=args.provider,
        reasoning=args.effort, toolsets=("clarify",), command=(args.command or "hermes",),
        timeout=args.timeout, telemetry_sink=telemetry, max_prompt_bytes=MAX_PROMPT_BYTES,
        max_output_bytes=256 * 1024)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runner", required=True, choices=RUNNERS)
    parser.add_argument("--stage", required=True, choices=STAGES)
    parser.add_argument("--name", required=True, help="worker name, also its capability")
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--bus-url", required=True)
    parser.add_argument("--model")
    parser.add_argument("--effort")
    parser.add_argument("--provider", help="hermes runner: provider, for example nous")
    parser.add_argument("--command", help="path to the claude, codex or hermes executable")
    parser.add_argument("--timeout", type=float, default=2400.0)
    parser.add_argument("--max-turns", type=int, default=100)
    parser.add_argument("--max-budget-usd", type=float, default=10.0)
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument("--heartbeat-seconds", type=float, default=5.0)
    parser.add_argument("--record", type=Path)
    parser.add_argument("--daybreak", action="store_true",
                        help="codex runner: ask for OpenAI's Daybreak handling on every run")
    args = parser.parse_args(argv)
    if args.daybreak and args.runner != "codex":
        parser.error("--daybreak applies only to the codex runner")
    if args.runner == "hermes" and not (args.model and args.provider):
        parser.error("the hermes runner needs --model and --provider")
    return args


def main(argv=None):
    args = parse_args(argv)
    instance_id = uuid.uuid4().hex
    bus = BusClient(args.bus_url, actor=args.name)
    telemetry = BusTelemetrySink(bus, producer=ProducerIdentity(
        f"examples.contest_hunt.{args.runner}", instance_id, "0.1.0"))
    executor = BriefingExecutor(build_executor(args, telemetry), workspace=args.workspace.resolve(),
                                stage=args.stage, inline=args.runner == "hermes")
    worker_executor = executor
    if args.record:
        from examples.openclaw.run_worker import RecordingExecutor
        worker_executor = RecordingExecutor(executor, args.record, report_key="agent_report")
    runtime = WorkerRuntime(bus, name=args.name, executor=worker_executor, instance_id=instance_id,
                            capacity=args.capacity, capabilities=[args.name],
                            heartbeat_seconds=args.heartbeat_seconds)
    print(f"[{args.name}] {args.runner} {args.model or 'default model'} for {args.stage}; "
          f"pid {os.getpid()}", flush=True)
    try:
        runtime.run()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", flush=True)
    finally:
        executor.close()
    # Non-zero only when the disconnect timeout stopped the worker, so a
    # supervisor can restart it on failure.
    if runtime.exit_code:
        raise SystemExit(runtime.exit_code)


if __name__ == "__main__":
    main()
