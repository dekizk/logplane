"""Start a Claude Code worker against an existing bus.

Choose how Claude Code authenticates with --auth:
  login    your normal Claude Code login (for example a subscription)
  api-key  claude --bare, billed to ANTHROPIC_API_KEY
"""

import argparse
import os
import uuid
from pathlib import Path

from examples.claude_code.claude_code_executor import AUTH_MODES, EFFORT_LEVELS, ClaudeCodeExecutor
from examples.openclaw.run_worker import RecordingExecutor
from logplane.client import BusClient
from logplane.runtime import WorkerRuntime
from logplane.telemetry import BusTelemetrySink, ProducerIdentity

ADAPTER_VERSION = "0.1.0"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--auth", required=True, choices=AUTH_MODES,
                        help="login: your Claude Code login; api-key: --bare with ANTHROPIC_API_KEY")
    parser.add_argument("--bus-url", default=os.environ.get("LOGPLANE_URL", "http://127.0.0.1:8765"))
    parser.add_argument("--name", default="claude")
    parser.add_argument("--capability", action="append", dest="capabilities", default=[],
                        help="advertised capability; repeat for more (default: claude)")
    parser.add_argument("--claude-command", default="claude")
    parser.add_argument("--working-directory", required=True, type=Path,
                        help="where Claude Code runs; with --read-only-tools, what it may read")
    parser.add_argument("--model", help="model alias or name; defaults to Claude Code's own default")
    parser.add_argument("--effort", choices=EFFORT_LEVELS)
    parser.add_argument("--read-only-tools", action="store_true",
                        help="allow Read, Grep and Glob in the working directory (default: no tools)")
    parser.add_argument("--timeout", type=float, default=300.0, help="seconds per assignment")
    parser.add_argument("--max-turns", type=int, default=4)
    parser.add_argument("--max-budget-usd", type=float, default=0.50,
                        help="Claude Code's own spending stop for one assignment")
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument("--heartbeat-seconds", type=float, default=2.0)
    parser.add_argument("--no-telemetry", action="store_true",
                        help="do not report usage to Logplane (workflow budgets will not see it)")
    parser.add_argument("--record", type=Path,
                        help="append received assignments and returned outcomes to this JSONL file")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    instance_id = uuid.uuid4().hex
    bus = BusClient(args.bus_url, actor=args.name)
    telemetry = None if args.no_telemetry else BusTelemetrySink(
        bus, producer=ProducerIdentity("examples.claude_code", instance_id, ADAPTER_VERSION))
    executor = ClaudeCodeExecutor(
        auth=args.auth, working_directory=args.working_directory,
        command=(args.claude_command,), model=args.model, effort=args.effort,
        read_only_tools=args.read_only_tools, timeout=args.timeout, max_turns=args.max_turns,
        max_budget_usd=args.max_budget_usd, telemetry_sink=telemetry,
    )
    worker_executor = (RecordingExecutor(executor, args.record, report_key="claude_report")
                       if args.record else executor)
    runtime = WorkerRuntime(bus, name=args.name, executor=worker_executor, instance_id=instance_id,
                            capacity=args.capacity, capabilities=args.capabilities or ["claude"],
                            heartbeat_seconds=args.heartbeat_seconds)
    print(f"[{args.name}] Claude Code worker, auth={args.auth}", flush=True)
    try:
        runtime.run()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", flush=True)
    finally:
        executor.close()


if __name__ == "__main__":
    main()
