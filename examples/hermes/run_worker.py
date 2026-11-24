"""Run Hermes Agent as a logplane worker."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

from logplane.artifacts import ArtifactStore
from logplane.client import BusClient
from examples.hermes.hermes_executor import HermesExecutor, REASONING_LEVELS
from logplane.runtime import WorkerRuntime
from logplane.telemetry import BusTelemetrySink, ProducerIdentity

HERMES_ADAPTER_VERSION = "0.7.0"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the optional Hermes Agent executor example"
    )
    parser.add_argument("--name", default="hermes")
    parser.add_argument(
        "--bus-url",
        default=os.environ.get("LOGPLANE_URL", "http://127.0.0.1:8765"),
    )
    parser.add_argument("--working-directory", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument(
        "--reasoning",
        choices=REASONING_LEVELS,
        help="optional Hermes reasoning effort override; requires model/provider support",
    )
    parser.add_argument(
        "--toolsets",
        default="clarify",
        help="explicit comma-separated Hermes toolsets; defaults to clarify only",
    )
    parser.add_argument(
        "--hermes-command",
        default="hermes",
        help="Hermes executable path",
    )
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument(
        "--capability",
        action="append",
        dest="capabilities",
        default=[],
        help="advertised capability; repeat for more than one",
    )
    parser.add_argument(
        "--unsafe-user-config",
        action="store_true",
        help="allow Hermes user rules/plugins/hooks instead of safe mode",
    )
    parser.add_argument(
        "--capture-content",
        action="store_true",
        help="opt in to storing prompts and model output as local artifacts",
    )
    parser.add_argument(
        "--artifact-directory",
        type=Path,
        default=Path(os.environ.get("LOGPLANE_ARTIFACT_DIR", "artifacts")),
        help="local content-addressed store used only with --capture-content",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    toolsets = tuple(item.strip() for item in args.toolsets.split(",") if item.strip())
    instance_id = uuid.uuid4().hex
    bus = BusClient(args.bus_url, actor=args.name)
    artifact_store = (
        ArtifactStore(args.artifact_directory) if args.capture_content else None
    )
    telemetry = BusTelemetrySink(
        bus,
        producer=ProducerIdentity(
            "examples.hermes",
            instance_id,
            HERMES_ADAPTER_VERSION,
        ),
        artifact_store=artifact_store,
        capture_content=args.capture_content,
    )

    def report_usage(assignment_id: str, usage: dict[str, object]) -> None:
        print(
            f"[hermes] usage {assignment_id}: "
            + json.dumps(usage, sort_keys=True, separators=(",", ":"))
        )

    executor = HermesExecutor(
        working_directory=args.working_directory,
        model=args.model,
        provider=args.provider,
        reasoning=args.reasoning,
        toolsets=toolsets,
        command=(args.hermes_command,),
        timeout=args.timeout,
        safe_mode=not args.unsafe_user_config,
        usage_callback=report_usage,
        telemetry_sink=telemetry,
    )
    runtime = WorkerRuntime(
        bus,
        name=args.name,
        executor=executor,
        instance_id=instance_id,
        capacity=args.capacity,
        capabilities=args.capabilities or ["hermes"],
        heartbeat_seconds=float(
            os.environ.get("LOGPLANE_HEARTBEAT_SECONDS", "5")
        ),
    )
    try:
        runtime.run()
    except KeyboardInterrupt:
        # A stop signal is the normal way to end a worker; leave a clean log.
        print("[hermes] stopped", flush=True)
    # Non-zero only when the disconnect timeout stopped the worker, so a
    # supervisor can restart it on failure.
    if runtime.exit_code:
        raise SystemExit(runtime.exit_code)


if __name__ == "__main__":
    main()
