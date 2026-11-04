"""Start the optional text-only OpenClaw worker against an existing bus."""

import argparse
from pathlib import Path

from examples.openclaw.openclaw_executor import OpenClawExecutor
from logplane.client import BusClient
from logplane.runtime import WorkerRuntime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bus-url", default="http://127.0.0.1:8765")
    parser.add_argument("--openclaw-command", default="openclaw")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--working-directory", required=True, type=Path)
    parser.add_argument("--model", default="nousportal/openai/gpt-6-luna")
    parser.add_argument("--thinking", default="high")
    parser.add_argument("--timeout", default=120.0, type=float)
    args = parser.parse_args()
    executor = OpenClawExecutor(
        command=(args.openclaw_command,), config=args.config, model=args.model,
        working_directory=args.working_directory, timeout=args.timeout, thinking=args.thinking,
    )
    runtime = WorkerRuntime(BusClient(args.bus_url, actor="openclaw"), name="openclaw",
                            executor=executor, capacity=1, capabilities=["openclaw"],
                            heartbeat_seconds=2)
    try:
        runtime.run()
    except KeyboardInterrupt:
        print("[openclaw] stopped")
    finally:
        executor.close()


if __name__ == "__main__":
    main()
