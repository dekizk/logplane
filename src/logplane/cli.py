"""Approachable operations, onboarding, and integration CLI for logplane."""

from __future__ import annotations

import argparse
import json
import math
import os
import shlex
import sys
import time
import uuid
from pathlib import Path
from typing import Optional, TextIO

import httpx

from logplane.client import BusProtocolError
from logplane.legacy import legacy_environment_error
from logplane.observer import ObserverClient
from logplane.operations import (
    ProjectionLookupError,
    build_projection,
    explain_task,
    discovery_view,
    operator_next_action,
    task_view,
    worker_views,
    workflow_mermaid,
    workflow_view,
)
from logplane.projection import CoordinationProjection, PROJECTION_TOPICS, apply_event
from logplane.scheduling import DEFAULT_TASK_PRIORITY, TASK_PRIORITY_CLASSES
from logplane.topics import TELEMETRY_TOPICS
from logplane.version import VERSION


DEFAULT_URL = "http://127.0.0.1:8765"
DEFAULT_LEASE_SECONDS = 20.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="logplane",
        description="Understand agent work from immutable event history.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    _add_common_options(parser, defaults=True)
    commands = parser.add_subparsers(dest="command", required=True)

    for name in ('tasks', 'workflows', 'decisions'):
        listing = commands.add_parser(name, help=f'list projected {name} without needing an ID')
        listing.add_argument('--limit', type=_positive_int, default=50, help='maximum rows (1–200)')
        if name == 'workflows':
            listing.add_argument('--after-workflow', default='', help='continue after this workflow ID')
        else:
            listing.add_argument('--after-task-id', type=_nonnegative_int, default=0)
            listing.add_argument('--workflow', help='filter by correlation ID')
        if name != 'decisions':
            listing.add_argument('--status', help='filter by exact projected status (tasks use raw status)')
        _add_common_options(listing)

    for name in ('cancel', 'retry', 'decide'):
        control = commands.add_parser(name, help=f'record a {name} intent and verify replay acceptance')
        control.add_argument('task_id', type=_positive_int)
        control.add_argument('--config', help='local config path; otherwise use normal URL resolution')
        control.add_argument('--idempotency-key', help='reuse unchanged when retrying this command')
        if name in ('cancel', 'retry'):
            control.add_argument('--reason', required=True)
        if name == 'retry':
            control.add_argument('--failed-event', required=True, type=_positive_int)
            control.add_argument('--additional-retries', required=True, type=_positive_int,
                                 help='grant this many new assignment opportunities')
        if name == 'decide':
            control.add_argument('--decision-event', required=True, type=_positive_int)
            control.add_argument('--value', required=True, help='JSON decision value (quote text as a JSON string)')
        _add_common_options(control)

    doctor = commands.add_parser(
        "doctor", help="check the bus and local coordination view"
    )
    _add_common_options(doctor)

    workers = commands.add_parser(
        "workers", help="show worker capacity and lease health"
    )
    _add_common_options(workers)

    task = commands.add_parser("task", help="show the current state of one task")
    task.add_argument("task_id", type=_positive_int)
    _add_common_options(task)

    workflow = commands.add_parser(
        "workflow", help="show one correlated workflow and DAG"
    )
    workflow.add_argument("correlation_id")
    workflow.add_argument(
        "--mermaid",
        action="store_true",
        help="emit a read-only Mermaid flowchart",
    )
    _add_common_options(workflow)

    explain = commands.add_parser(
        "explain", help="explain why one task can or cannot advance"
    )
    explain.add_argument("task_id", type=_positive_int)
    _add_common_options(explain)

    tail = commands.add_parser("tail", help="follow events for one correlated workflow")
    tail.add_argument("correlation_id")
    tail.add_argument("--from-id", type=_nonnegative_int, default=0)
    _add_common_options(tail)

    init = commands.add_parser(
        "init", help="create a safe local logplane configuration"
    )
    init.add_argument("directory", nargs="?", default=".")
    _add_output_option(init)

    serve = commands.add_parser("serve", help="run the local event bus")
    serve.add_argument("--config", default="logplane.local.json")

    up = commands.add_parser(
        "up", help="run the bus and the coordinator together (Ctrl-C stops both)")
    up.add_argument("--config", default="logplane.local.json")
    up.add_argument("--snapshot", action="store_true", help="use a disposable local replay snapshot")
    up.add_argument("--quiet", action="store_true",
                    help="keep coordinator output in its log file only")

    pm = commands.add_parser("pm", help="run the local orchestration manager")
    pm.add_argument("--config", default="logplane.local.json")
    pm.add_argument("--snapshot", action="store_true", help="use a disposable local replay snapshot")

    for name in ("storage-audit", "backup"):
        command = commands.add_parser(name, help="audit local artifacts without deleting" if name == "storage-audit" else "create a verified database/artifact recovery bundle")
        command.add_argument("--config", default="logplane.local.json")
        command.add_argument("--artifact-root", action="append", default=[])
        if name == "backup":
            command.add_argument("destination")
        else:
            command.add_argument("--min-age-days", type=_nonnegative_int, default=7)
        _add_output_option(command)
    for name in ("verify-backup", "restore"):
        command = commands.add_parser(name, help="verify a backup bundle" if name == "verify-backup" else "restore a bundle into a NEW directory")
        command.add_argument("bundle")
        if name == "restore":
            command.add_argument("destination")
        _add_output_option(command)

    snapshot = commands.add_parser("snapshot", help="build or clear the local coordination replay cache")
    snapshot.add_argument("action", choices=["build", "clear"])
    snapshot.add_argument("--config", default="logplane.local.json")
    _add_output_option(snapshot)

    rebuild = commands.add_parser(
        "rebuild-task-index", help="rebuild local task lookup from immutable history"
    )
    rebuild.add_argument("--config", default="logplane.local.json")
    _add_output_option(rebuild)

    demo = commands.add_parser(
        "demo-worker", help="run a bounded worker for the local quick start"
    )
    demo.add_argument("name")
    demo.add_argument("--config", default="logplane.local.json")
    demo.add_argument("--capacity", type=_positive_int, default=1)
    demo.add_argument("--capability", action="append", default=[])
    demo_outcome = demo.add_mutually_exclusive_group()
    demo_outcome.add_argument(
        "--block", type=_positive_int, metavar="TASK_ID",
        help="block this task once and ask a human for a decision",
    )
    demo_outcome.add_argument(
        "--fail", type=_positive_int, metavar="TASK_ID",
        help="report one retryable failure for this task",
    )
    demo_outcome.add_argument(
        "--fail-permanently", type=_positive_int, metavar="TASK_ID",
        help="report one permanent failure for this task",
    )
    demo.add_argument(
        "--delay", type=_nonnegative_number, default=0.5, metavar="SECONDS",
        help="simulated work time per task (default 0.5); raise it to kill a worker mid-task",
    )

    demo_command = commands.add_parser(
        "demo", help="run a scripted, self-contained demonstration scenario",
        description="Start a throwaway bus, PM, and workers, then play one scenario. "
        "Run without a scenario to list them.",
    )
    demo_command.add_argument("scenario", nargs="?", help="recovery, approval, budget, or audit-triage")
    demo_command.add_argument(
        "--pace", type=_positive_number, default=1.0,
        help="time multiplier for simulated work; 0.5 runs about twice as fast",
    )
    demo_command.add_argument(
        "--yes", action="store_true",
        help="answer human decisions with the default instead of prompting",
    )
    demo_command.add_argument(
        "--dir", help="new or empty directory for the demo history (default: a temp directory)",
    )
    demo_command.add_argument(
        "--keep-running", action=argparse.BooleanOptionalAction, default=None,
        help="leave the bus running for exploration until Enter (default: when interactive)",
    )
    demo_command.add_argument(
        "--read-pauses", action="store_true",
        help="pause after each narration long enough to read it (for recordings and talks)",
    )

    submit = commands.add_parser("submit", help="submit one controlled local task")
    submit.add_argument("title")
    submit.add_argument("--config", default="logplane.local.json")
    submit.add_argument("--context", default="{}", help="JSON object passed to the agent")
    submit.add_argument("--capability", action="append", default=[])
    submit.add_argument(
        "--depends-on", action="append", default=[], type=_positive_int, metavar="TASK_ID",
        help="upstream task that must complete first; repeat for several",
    )
    submit.add_argument("--max-retries", type=_nonnegative_int, default=0)
    submit.add_argument(
        "--priority",
        choices=TASK_PRIORITY_CLASSES,
        default=DEFAULT_TASK_PRIORITY,
    )
    submit.add_argument(
        "--not-before",
        type=_positive_number,
        help="absolute Unix timestamp before which the task is ineligible",
    )
    submit.add_argument("--correlation-id")
    submit.add_argument(
        "--idempotency-key",
        help="stable identity to make a retried submission safe",
    )
    _add_output_option(submit)

    for command_name, help_text in (
        ("pause", "pause one task or an entire workflow"),
        ("resume", "resume one paused task or workflow"),
    ):
        control = commands.add_parser(command_name, help=help_text)
        control.add_argument("scope", choices=("task", "workflow"))
        control.add_argument("target", help="task id or workflow correlation id")
        control.add_argument("--reason", required=True)
        control.add_argument("--config", default="logplane.local.json")
        control.add_argument("--idempotency-key")
        _add_output_option(control)

    supersede = commands.add_parser(
        "supersede", help="replace immutable task intent with a new task"
    )
    supersede.add_argument("task_id", type=_positive_int)
    supersede.add_argument("title", help="title for the replacement task")
    supersede.add_argument("--reason", required=True)
    supersede.add_argument("--config", default="logplane.local.json")
    supersede.add_argument("--context", default="{}")
    supersede.add_argument("--capability", action="append", default=[])
    supersede.add_argument("--max-retries", type=_nonnegative_int, default=0)
    supersede.add_argument(
        "--priority", choices=TASK_PRIORITY_CLASSES, default=DEFAULT_TASK_PRIORITY
    )
    supersede.add_argument("--not-before", type=_positive_number)
    supersede.add_argument("--idempotency-key")
    _add_output_option(supersede)

    policy = commands.add_parser(
        "policy", help="set immutable scheduling policy for one workflow"
    )
    policy.add_argument("correlation_id")
    concurrency = policy.add_mutually_exclusive_group()
    concurrency.add_argument(
        "--max-active-assignments",
        type=_positive_int,
        default=argparse.SUPPRESS,
        help="maximum simultaneously assigned or running tasks",
    )
    concurrency.add_argument(
        "--unbounded-concurrency",
        "--unbounded",
        dest="max_active_assignments",
        action="store_const",
        const=None,
        default=argparse.SUPPRESS,
        help="remove the workflow concurrency limit",
    )
    for option, destination, value_type, label in (
        ("tokens", "max_total_tokens", _positive_int, "token"),
        ("cost", "max_total_cost_usd", _positive_number, "cost"),
        ("attempts", "max_attempts", _positive_int, "attempt"),
        (
            "wall-clock",
            "max_wall_clock_seconds",
            _positive_number,
            "wall-clock",
        ),
    ):
        group = policy.add_mutually_exclusive_group()
        group.add_argument(
            f"--max-{option}",
            dest=destination,
            type=value_type,
            default=argparse.SUPPRESS,
            help=f"maximum workflow {label} budget",
        )
        group.add_argument(
            f"--unbounded-{option}",
            dest=destination,
            action="store_const",
            const=None,
            default=argparse.SUPPRESS,
            help=f"remove the workflow {label} budget",
        )
    policy.add_argument(
        "--reserve-tokens-per-assignment",
        type=_nonnegative_int,
        default=argparse.SUPPRESS,
    )
    policy.add_argument(
        "--reserve-cost-per-assignment",
        dest="reserve_cost_usd_per_assignment",
        type=_nonnegative_number,
        default=argparse.SUPPRESS,
    )
    policy.add_argument("--reason", required=True)
    policy.add_argument("--config", default="logplane.local.json")
    policy.add_argument("--idempotency-key")
    _add_output_option(policy)

    agent_policy = commands.add_parser(
        "agent-policy", help="set an immutable concurrency policy for one agent"
    )
    agent_policy.add_argument("agent_name")
    agent_concurrency = agent_policy.add_mutually_exclusive_group(required=True)
    agent_concurrency.add_argument(
        "--max-active-assignments", type=_positive_int
    )
    agent_concurrency.add_argument(
        "--unbounded", action="store_true"
    )
    agent_policy.add_argument("--reason", required=True)
    agent_policy.add_argument("--config", default="logplane.local.json")
    agent_policy.add_argument("--idempotency-key")
    _add_output_option(agent_policy)

    adapter = commands.add_parser(
        "adapter", help="check or run an existing agent integration"
    )
    adapter_commands = adapter.add_subparsers(dest="adapter_command", required=True)
    adapter_check = adapter_commands.add_parser(
        "check", help="run the standalone adapter contract probe"
    )
    target = adapter_check.add_mutually_exclusive_group(required=True)
    target.add_argument("--config", dest="integration_config")
    target.add_argument("--python-target", metavar="MODULE:ATTRIBUTE")
    adapter_check.add_argument(
        "--timeout", type=_positive_number, default=30.0,
        help="total check timeout in seconds, including loading and cleanup (default: 30)",
    )
    _add_output_option(adapter_check)
    adapter_run = adapter_commands.add_parser(
        "run", help="run a configuration-driven agent worker"
    )
    adapter_run.add_argument("integration_config")
    adapter_run.add_argument(
        "--local-config", default="logplane.local.json"
    )
    return parser


def _add_common_options(parser: argparse.ArgumentParser, *, defaults: bool = False) -> None:
    default = None if defaults else argparse.SUPPRESS
    parser.add_argument(
        "--url",
        default=(None if defaults else default),
        help=(
            "bus URL (default: local config, then LOGPLANE_URL, then "
            "http://127.0.0.1:8765)"
        ),
    )
    parser.add_argument(
        "--token",
        default=(os.environ.get("LOGPLANE_TOKEN") if defaults else default),
        help="bearer token (default: LOGPLANE_TOKEN)",
    )
    parser.add_argument(
        "--lease-seconds",
        type=_positive_number,
        default=(_lease_default() if defaults else default),
        help="worker lease duration used for health explanations",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=(False if defaults else default),
        help="emit machine-readable JSON",
    )


def _add_output_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit machine-readable JSON",
    )


def main(
    argv: Optional[list[str]] = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
    clock=time.time,
    client_factory=ObserverClient,
    local_config_path: Optional[str | Path] = None,
) -> int:
    args = build_parser().parse_args(argv)
    legacy_error = legacy_environment_error()
    if legacy_error:
        print(f"logplane: {legacy_error}", file=stderr)
        return 2
    if args.command == "demo":
        from logplane import demo
        return demo.run(
            args.scenario,
            out=stdout,
            pace=args.pace,
            assume_yes=args.yes,
            directory=args.dir,
            keep_running=args.keep_running,
            read_pauses=args.read_pauses,
        )
    if args.command in {
        "init", "up", "serve", "pm", "demo-worker", "submit", "pause", "resume",
        "supersede", "policy", "agent-policy", "adapter", "rebuild-task-index", "snapshot",
        "storage-audit", "backup", "verify-backup", "restore",
    }:
        try:
            return _run_local_command(args, stdout=stdout, stderr=stderr)
        except httpx.HTTPError as exc:
            print(f"logplane: could not reach local bus: {_friendly_error(exc)}", file=stderr)
            return 2
        except (OSError, json.JSONDecodeError, ValueError, BusProtocolError) as exc:
            print(f"logplane: {exc}", file=stderr)
            return 2
        except KeyboardInterrupt:
            return 130

    try:
        args.url = _resolve_observer_url(
            args.url,
            local_config_path=getattr(args, 'config', None) or local_config_path,
        )
    except ValueError as exc:
        print(f"logplane: {exc}", file=stderr)
        return 2

    client = client_factory(args.url, token=args.token)
    try:
        if args.command in {'tasks', 'workflows', 'decisions'}:
            from logplane.operator_controls import read_snapshot
            state, head, _, _, _ = read_snapshot(client)
            value = discovery_view(state, kind=args.command, now=clock(), lease_seconds=args.lease_seconds,
                                   limit=args.limit, after_task_id=getattr(args, 'after_task_id', 0),
                                   after_workflow=getattr(args, 'after_workflow', ''),
                                   status=getattr(args, 'status', None), correlation_id=getattr(args, 'workflow', None))
            value['observed_through_id'] = head
            _render_simple(value, args, stdout, _format_discovery(value, args.command))
        elif args.command in {'cancel', 'retry', 'decide'}:
            from logplane.operator_controls import intervene

            def publish(intent):
                headers = {'Authorization': f'Bearer {args.token}'} if args.token else {}
                response = httpx.post(f'{args.url}/events', json=intent, headers=headers, timeout=10)
                response.raise_for_status()
                return response.json()

            value = intervene(client, publish, command=args.command, task_id=args.task_id,
                              reason=getattr(args, 'reason', None),
                              anchor=getattr(args, 'failed_event', getattr(args, 'decision_event', None)),
                              additional_retries=getattr(args, 'additional_retries', None),
                              decision=json.loads(args.value) if args.command == 'decide' else None,
                              idempotency_key=args.idempotency_key, now=clock())
            _render_simple(value, args, stdout,
                           f"Recorded {value['event']['topic']} · event #{value['event']['id']}\n"
                           f"{value['message']}\nTask state at event #{value['observed_through_id']}: {value['task_status']}\n"
                           f"Next: {shlex.join(value['next_action'])}")
            return 0 if value['accepted'] else 4
        elif args.command == "doctor":
            value = _doctor(client, now=clock(), lease_seconds=args.lease_seconds)
            _render(value, args, stdout, _format_doctor)
        elif args.command == "workers":
            state, _ = _coordination_snapshot(client)
            value = {
                "workers": worker_views(
                    state,
                    now=clock(),
                    lease_seconds=args.lease_seconds,
                )
            }
            _render(value, args, stdout, _format_workers)
        elif args.command == "task":
            state, _ = _coordination_snapshot(client)
            value = task_view(
                state,
                args.task_id,
                now=clock(),
                lease_seconds=args.lease_seconds,
            )
            _render(value, args, stdout, _format_task)
        elif args.command == "explain":
            state, _ = _coordination_snapshot(client)
            value = {
                "task_id": args.task_id,
                **explain_task(
                    state,
                    args.task_id,
                    now=clock(),
                    lease_seconds=args.lease_seconds,
                ),
                "next_action": operator_next_action(state.tasks[args.task_id], now=clock()),
            }
            _render(value, args, stdout, _format_explanation)
        elif args.command == "workflow":
            if args.mermaid and args.json:
                print("logplane: --mermaid and --json cannot be combined", file=stderr)
                return 2
            state, _ = _coordination_snapshot(client)
            telemetry = (
                []
                if args.mermaid
                else client.query_all(
                    topics=sorted(TELEMETRY_TOPICS),
                    correlation_id=args.correlation_id,
                )
            )
            value = workflow_view(
                state,
                args.correlation_id,
                now=clock(),
                lease_seconds=args.lease_seconds,
                telemetry_events=telemetry,
            )
            if args.mermaid:
                print(workflow_mermaid(value), file=stdout)
            else:
                _render(value, args, stdout, _format_workflow)
        elif args.command == "tail":
            return _tail(client, args, stdout)
        return 0
    except ProjectionLookupError as exc:
        print(f"logplane: {exc}", file=stderr)
        return 3
    except (httpx.HTTPError, BusProtocolError, json.JSONDecodeError, ValueError) as exc:
        if args.command in {'cancel', 'retry', 'decide'}:
            print(f"logplane: intervention failed: {_friendly_error(exc)}. If publication was interrupted, "
                  "retry the identical command/key; do not assume it was not recorded.", file=stderr)
        else:
            print(f"logplane: could not read {args.url}: {_friendly_error(exc)}", file=stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def _resolve_observer_url(
    explicit_url: Optional[str],
    *,
    local_config_path: Optional[str | Path] = None,
) -> str:
    if explicit_url is not None:
        if not isinstance(explicit_url, str) or not explicit_url.strip():
            raise ValueError("--url must be a non-empty string")
        return explicit_url.rstrip("/")

    from logplane.local_config import DEFAULT_LOCAL_CONFIG, LocalConfig

    path = (
        Path(local_config_path).expanduser()
        if local_config_path is not None
        else Path.cwd() / DEFAULT_LOCAL_CONFIG
    )
    environment_url = os.environ.get("LOGPLANE_URL")
    if path.exists():
        configured_url = LocalConfig.from_file(path).bus_url.rstrip("/")
        if environment_url and environment_url.rstrip("/") != configured_url:
            raise ValueError(
                f"LOGPLANE_URL ({environment_url}) conflicts with local config "
                f"({configured_url}); unset LOGPLANE_URL or pass --url explicitly"
            )
        return configured_url
    from logplane.legacy import legacy_config_hint

    hint = legacy_config_hint(path.resolve())
    if hint:
        # Falling back to LOGPLANE_URL or the default port here could reach a
        # different bus than the one the old config describes.
        raise ValueError(f"{hint}; or pass --url to choose a bus explicitly")
    if environment_url:
        return environment_url.rstrip("/")
    return DEFAULT_URL


def _run_local_command(
    args: argparse.Namespace,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    """Run onboarding and integration commands before creating an observer."""
    if args.command in {"storage-audit", "backup", "verify-backup", "restore"}:
        import sqlite3
        from logplane.recovery import audit, backup, verify_bundle, restore
        from logplane.local_config import LocalConfig

        try:
            if args.command == "verify-backup":
                value, _ = verify_bundle(args.bundle)
                value = {"ok": True, **value}
            elif args.command == "restore":
                value = {"ok": True, **restore(args.bundle, args.destination)}
            else:
                local = LocalConfig.from_file(args.config)
                if args.command == "backup":
                    value = {"ok": True, **backup(local.database_path, args.destination, args.artifact_root)}
                else:
                    value = audit(local.database_path, args.artifact_root, min_age_days=args.min_age_days)
        except sqlite3.Error as exc:
            raise ValueError(f"storage operation failed: {exc}; preserve the source and inspect any .incomplete output") from exc
        _render_simple(value, args, stdout, json.dumps(value, indent=2, sort_keys=True))
        return 0 if value.get("ok") else 4

    if args.command == "init":
        from logplane.local_config import initialize_local_config

        path = initialize_local_config(args.directory)
        value = {"ok": True, "config": str(path)}
        _render_simple(value, args, stdout, f"Created {path}\nNext: logplane serve --config {path}")
        return 0

    if args.command == "adapter" and args.adapter_command == "check":
        from logplane.adapter_check import run_adapter_check

        report = run_adapter_check(python_target=args.python_target,
                                   integration_config=args.integration_config,
                                   timeout=args.timeout)
        value = report.to_dict()
        lines = [f"Adapter: {'PASS' if report.ok else 'FAIL'}"]
        lines.extend(
            f"  {'ok' if check.ok else 'FAIL'} · {check.name}: {check.detail}"
            for check in report.checks
        )
        _render_simple(value, args, stdout, "\n".join(lines))
        return 0 if report.ok else 4

    from logplane.local_config import LocalConfig

    local_path = (
        args.local_config
        if args.command == "adapter" and args.adapter_command == "run"
        else args.config
    )
    local = LocalConfig.from_file(local_path)
    local.apply_environment()

    if args.command == "snapshot":
        import sqlite3
        from logplane.projection_store import load_projection, save_projection, clear_projection

        try:
            if args.action == "clear":
                clear_projection(local.database_path)
                value = {"ok": True, "action": "clear"}
                message = "Cleared coordination snapshot; event history unchanged."
            else:
                result = load_projection(local.database_path, use_snapshot=False)
                saved = save_projection(local.database_path, result)
                if not saved:
                    raise ValueError("projection exceeds the 64 MiB snapshot limit; full replay remains available")
                value = {"ok": True, "action": "build", "through_id": result.last_event_id,
                         "replayed_events": result.replayed_events}
                message = f"Built coordination snapshot through event #{result.last_event_id}."
        except sqlite3.Error as exc:
            raise ValueError(f"snapshot operation failed: {exc}; start the upgraded bus first") from exc
        _render_simple(value, args, stdout, message)
        return 0

    if args.command == "rebuild-task-index":
        from logplane import bus
        import sqlite3

        if not local.database_path.is_file():
            raise ValueError(f"database does not exist: {local.database_path}")
        previous_path = bus.DB_PATH
        try:
            bus.DB_PATH = local.database_path
            count = bus.rebuild_task_index()
        except sqlite3.Error as exc:
            raise ValueError(f"task index rebuild failed (rolled back): {exc}") from exc
        finally:
            bus.DB_PATH = previous_path
        _render_simple(
            {"ok": True, "tasks_indexed": count, "database": str(local.database_path)},
            args, stdout, f"Rebuilt task index: {count} tasks; event history unchanged.",
        )
        return 0

    if args.command == "up":
        from logplane.up import run_up

        return run_up(Path(args.config), snapshot=args.snapshot, quiet=args.quiet,
                      stdout=stdout, stderr=stderr)
    if args.command == "serve":
        import uvicorn

        from logplane import bus
        class BusServer(uvicorn.Server):
            """uvicorn server that ends open event streams on the first stop signal."""

            def handle_exit(self, sig, frame):
                bus.begin_shutdown()
                super().handle_exit(sig, frame)

        server = BusServer(uvicorn.Config("logplane.bus:app", host=local.host, port=local.port))
        startup_failure = ValueError(
            f"the bus could not start on {local.bus_url}; check the log above "
            "(is the port already in use?)"
        )
        try:
            server.run()
        except SystemExit:
            # uvicorn exits by itself when startup fails (status 1 or 3,
            # depending on its version); report the documented usage status.
            if server.started:
                raise
            raise startup_failure from None
        if not server.started:
            raise startup_failure
        return 0
    if args.command == "pm":
        # Import only after the local environment is selected: pm_agent reads
        # its authoritative URL and lease settings at import time.
        from logplane import pm_agent
        pm_agent.main(snapshot_path=local.database_path if args.snapshot else None)
        return 0
    if args.command == "demo-worker":
        from logplane.client import BusClient
        from logplane.runtime import WorkerRuntime
        from logplane.worker import DemoExecutor

        runtime = WorkerRuntime(
            BusClient(local.bus_url, actor=args.name),
            name=args.name,
            executor=DemoExecutor(
                args.name,
                block_once=args.block,
                fail_once=args.fail,
                fail_permanently_once=args.fail_permanently,
                delay_seconds=args.delay,
            ),
            capacity=args.capacity,
            capabilities=args.capability,
        )
        runtime.run()
        return 0
    if args.command == "submit":
        from logplane.client import BusClient

        context = json.loads(args.context)
        if not isinstance(context, dict):
            raise ValueError("--context must decode to a JSON object")
        payload = {
            "title": args.title,
            "goal": args.title,
            "context": context,
            "required_capabilities": args.capability,
            "retry_policy": {"max_retries": args.max_retries},
            "priority": args.priority,
        }
        if args.not_before is not None:
            payload["not_before"] = args.not_before
        if args.depends_on:
            payload["depends_on"] = args.depends_on
        bus = BusClient(local.bus_url, actor="human")
        event = bus.publish(
            "task.created",
            payload,
            correlation_id=args.correlation_id,
            idempotency_key=args.idempotency_key or f"submit:{uuid.uuid4().hex}",
        )
        _render_simple(
            event,
            args,
            stdout,
            f"Submitted task {event['payload']['task_id']} · workflow {event['correlation_id']} · event #{event['id']}",
        )
        return 0
    if args.command in {"pause", "resume"}:
        from logplane.client import BusClient

        bus = BusClient(local.bus_url, actor="human")
        method_name = f"{args.command}_{args.scope}"
        method = getattr(bus, method_name)
        if args.scope == "task":
            try:
                target = int(args.target)
            except ValueError as exc:
                raise ValueError("task target must be a positive integer") from exc
            if target <= 0:
                raise ValueError("task target must be a positive integer")
        else:
            target = args.target
        event = method(
            target,
            reason=args.reason,
            idempotency_key=args.idempotency_key,
        )
        _render_simple(
            event,
            args,
            stdout,
            f"Recorded {args.command} request for {args.scope} {args.target} · event #{event['id']}",
        )
        return 0
    if args.command == "supersede":
        from logplane.client import BusClient

        context = json.loads(args.context)
        if not isinstance(context, dict):
            raise ValueError("--context must decode to a JSON object")
        replacement = {
            "title": args.title,
            "goal": args.title,
            "context": context,
            "required_capabilities": args.capability,
            "retry_policy": {"max_retries": args.max_retries},
            "priority": args.priority,
        }
        if args.not_before is not None:
            replacement["not_before"] = args.not_before
        event = BusClient(local.bus_url, actor="human").supersede_task(
            args.task_id,
            replacement,
            reason=args.reason,
            idempotency_key=args.idempotency_key,
        )
        _render_simple(
            event,
            args,
            stdout,
            f"Task {args.task_id} superseded by task {event['payload']['task_id']} · workflow {event['correlation_id']} · event #{event['id']}",
        )
        return 0
    if args.command == "policy":
        from logplane.client import BusClient

        field_names = (
            "max_active_assignments",
            "max_total_tokens",
            "max_total_cost_usd",
            "max_attempts",
            "max_wall_clock_seconds",
            "reserve_tokens_per_assignment",
            "reserve_cost_usd_per_assignment",
        )
        changes = {
            field_name: getattr(args, field_name)
            for field_name in field_names
            if hasattr(args, field_name)
        }
        if not changes:
            raise ValueError("at least one workflow policy option is required")
        event = BusClient(local.bus_url, actor="human").set_workflow_policy(
            args.correlation_id,
            reason=args.reason,
            idempotency_key=args.idempotency_key,
            **changes,
        )
        _render_simple(
            event,
            args,
            stdout,
            (
                f"Workflow {args.correlation_id} policy updated "
                f"({', '.join(changes)}) · event #{event['id']}"
            ),
        )
        return 0
    if args.command == "agent-policy":
        from logplane.client import BusClient

        limit = None if args.unbounded else args.max_active_assignments
        event = BusClient(local.bus_url, actor="human").set_agent_policy(
            args.agent_name,
            max_active_assignments=limit,
            reason=args.reason,
            idempotency_key=args.idempotency_key,
        )
        rendered = "unbounded" if limit is None else f"max {limit} active"
        _render_simple(
            event,
            args,
            stdout,
            f"Agent {args.agent_name} policy set to {rendered} · event #{event['id']}",
        )
        return 0
    if args.command == "adapter" and args.adapter_command == "run":
        from logplane.client import BusClient
        from logplane.integration import IntegrationConfig
        from logplane.runtime import WorkerRuntime

        integration = IntegrationConfig.from_file(args.integration_config)
        runtime = WorkerRuntime(
            BusClient(local.bus_url, actor=integration.worker_name),
            name=integration.worker_name,
            executor=integration.build_executor(),
            capacity=integration.capacity,
            capabilities=integration.capabilities,
        )
        runtime.run()
        return 0
    print("logplane: unsupported command", file=stderr)
    return 2


def _render_simple(value: dict, args: argparse.Namespace, stdout: TextIO, text: str) -> None:
    if args.json:
        print(json.dumps(value, indent=2, sort_keys=True), file=stdout)
    else:
        print(text, file=stdout)


def _coordination_snapshot(client: ObserverClient):
    events = client.query_all(topics=list(PROJECTION_TOPICS))
    return build_projection(events), events


def _format_discovery(value, kind):
    rows = value[kind]
    lines = [f"{kind.capitalize()} · {len(rows)} shown · through event #{value['observed_through_id']}"]
    if not rows:
        lines.append('No matching entries.')
    for row in rows:
        if kind == 'workflows':
            lines.append(f"{row['correlation_id']} · {row['status']} · {row['task_count']} tasks")
        else:
            lines.append(f"Task {row['task_id']} · {row['effective_status']} · {row['title']}")
            lines.append(f"  Workflow: {row['correlation_id'] or '(uncorrelated legacy task)'}")
            if kind == 'decisions':
                lines.append(f"  Decision event #{row['decision_event_id']} · {row['reason']}")
            else:
                lines.append(f"  {row['explanation']['summary']}")
            lines.append(f"  Next: {shlex.join(row['next_action'])}")
    cursor = value.get('next_after_task_id', value.get('next_after_workflow'))
    if cursor is not None:
        flag = '--after-workflow' if kind == 'workflows' else '--after-task-id'
        lines.append(f'More entries: repeat with {flag} {shlex.quote(str(cursor))}, preserving your filters.')
    return '\n'.join(lines)


def _doctor(client: ObserverClient, *, now: float, lease_seconds: float) -> dict:
    health = client.health()
    state, events = _coordination_snapshot(client)
    workers = worker_views(state, now=now, lease_seconds=lease_seconds)
    warnings = []
    if any(
        task.status == "open" and not state.workflow_is_paused(task.correlation_id)
        for task in state.tasks.values()
    ) and not any(
        worker["status"] == "healthy" for worker in workers
    ):
        warnings.append("No worker has a healthy lease; open work cannot be assigned.")
    reconciliation_codes = {
        "active_lease_expired",
        "active_worker_missing",
        "active_worker_replaced",
        "cancellation_pending",
        "pause_pending",
        "resume_pending",
        "supersession_pending",
        "workflow_pause_requested",
        "workflow_resume_requested",
        "deadline_reconciliation_pending",
        "decision_request_pending",
        "dependency_failure_reconciliation_pending",
        "permanent_failure_reconciliation_pending",
        "ready_for_assignment",
        "retry_exhaustion_reconciliation_pending",
    }
    pending_reconciliation = sum(
        explain_task(
            state,
            task.task_id,
            now=now,
            lease_seconds=lease_seconds,
        )["code"]
        in reconciliation_codes
        for task in state.tasks.values()
    )
    from logplane.pm_lock import pm_status

    pm = pm_status(client.base_url)
    if pending_reconciliation:
        if pm["status"] in {"not_running", "not_found"}:
            warnings.append(
                f"{pending_reconciliation} task(s) await the coordinator, and none is running; "
                "start it with `logplane pm` or `logplane up`."
            )
        else:
            warnings.append(
                f"{pending_reconciliation} task(s) appear to await PM reconciliation; verify the PM is running."
            )
    # Reducer validity is diagnosed in one linear replay below; avoid treating
    # deliberately stale historical outcomes as corruption.
    invalid_count = _ignored_event_count(events)
    return {
        "ok": bool(health.get("ok")),
        "bus_url": client.base_url,
        "schema_version": health.get("schema_version"),
        "coordination_events": len(events),
        "tasks": len(state.tasks),
        "workers": len(state.workers),
        "healthy_workers": sum(worker["status"] == "healthy" for worker in workers),
        "stale_workers": sum(worker["status"] == "stale" for worker in workers),
        "ignored_replay_events": invalid_count,
        "pending_reconciliation": pending_reconciliation,
        "pm": pm,
        "warnings": warnings,
    }


def _ignored_event_count(events: list[dict]) -> int:
    state = CoordinationProjection()
    ignored = 0
    for event in events:
        if not apply_event(state, event):
            ignored += 1
    return ignored


def _tail(client: ObserverClient, args: argparse.Namespace, stdout: TextIO) -> int:
    for event in client.subscribe(
        from_id=args.from_id,
        correlation_id=args.correlation_id,
    ):
        if args.json:
            print(json.dumps(event, sort_keys=True, separators=(",", ":")), file=stdout)
        else:
            payload = (
                event.get("payload")
                if isinstance(event.get("payload"), dict)
                else {}
            )
            identity = []
            if "task_id" in payload:
                identity.append(f"task {payload['task_id']}")
            if "assignment_id" in payload:
                identity.append(str(payload["assignment_id"]))
            suffix = f" · {' · '.join(identity)}" if identity else ""
            print(
                f"#{event.get('id')} {event.get('topic')} by {event.get('actor')}{suffix}",
                file=stdout,
                flush=True,
            )
    return 0


def _render(value: dict, args: argparse.Namespace, stdout: TextIO, formatter) -> None:
    if args.json:
        print(json.dumps(value, indent=2, sort_keys=True), file=stdout)
    else:
        print(formatter(value), file=stdout)


def _format_doctor(value: dict) -> str:
    lines = [
        f"Bus: {'healthy' if value['ok'] else 'unhealthy'} · {value['bus_url']} · schema v{value['schema_version']}",
        f"History: {value['coordination_events']} coordination events · {value['tasks']} tasks",
        f"Workers: {value['healthy_workers']} healthy · {value['stale_workers']} stale",
    ]
    pm = value.get("pm") or {}
    status = pm.get("status")
    if status == "running":
        lines.append(f"Coordinator: running ({pm['detail']})")
    elif status in {"not_running", "not_found"}:
        lines.append("Coordinator: not running · start it with `logplane pm`, or run "
                     "`logplane up` for the bus and coordinator together")
    elif status == "unknown":
        lines.append(f"Coordinator: unknown ({pm['detail']})")
    if value["ignored_replay_events"]:
        lines.append(
            f"Replay: {value['ignored_replay_events']} historical event(s) were safely ignored as stale or invalid"
        )
    lines.extend(f"Warning: {warning}" for warning in value["warnings"])
    return "\n".join(lines)


def _format_workers(value: dict) -> str:
    workers = value["workers"]
    if not workers:
        return "No workers have registered."
    lines = []
    for worker in workers:
        capabilities = ", ".join(worker["capabilities"]) or "none"
        lines.append(
            f"{worker['name']} · {worker['status']} · load {worker['load']}/{worker['effective_capacity']}"
            + (
                f" (advertised {worker['capacity']})"
                if worker["effective_capacity"] != worker["capacity"]
                else ""
            )
            + " · "
            f"lease {worker['lease_age_seconds']:.1f}s · capabilities: {capabilities} · event #{worker['last_event_id']}"
        )
    return "\n".join(lines)


def _format_task(value: dict) -> str:
    retry = value["retry_policy"]
    remaining = "unbounded" if retry["remaining"] is None else retry["remaining"]
    lines = [
        f"Task {value['task_id']} · {value['title']}",
        "State: "
        + value["effective_status"]
        + (
            f" (task state {value['status']})"
            if value["effective_status"] != value["status"]
            else ""
        )
        + f" · event #{value['status_event_id']}",
        f"Why: {value['explanation']['summary']}",
        f"Workflow: {value['correlation_id']}",
        f"Scheduling: priority {value['priority']}"
        + (
            f" · not before {value['not_before']}"
            if value["not_before"] is not None
            else ""
        ),
        f"Attempts: {value['attempt']} · retryable failures {retry['retryable_failures']} · retries remaining {remaining}",
    ]
    if value["assignment_id"]:
        label = "Current owner" if value["assignment_active"] else "Last attempt"
        lines.append(
            f"{label}: {value['assignee']} ({value['worker_instance_id']}) · "
            f"{value['assignment_id']} · event #{value['assignment_event_id']}"
        )
        fairness = value.get("assignment_fairness", {})
        if fairness.get("policy"):
            previous = fairness.get("previous_assignment_event_id")
            lines.append(
                "Fairness: workflow round-robin · previous assignment "
                + (f"#{previous}" if previous is not None else "none")
            )
        reservation = value.get("budget_reservation", {})
        if reservation.get("tokens") or reservation.get("cost_usd"):
            lines.append(
                f"Reservation: {reservation.get('tokens', 0)} tokens · "
                f"${reservation.get('cost_usd', 0):.6f}"
            )
    policy = value["workflow_policy"]
    if policy["status"] == "active":
        limit = policy["max_active_assignments"]
        lines.append(
            "Workflow policy: "
            + ("unbounded" if limit is None else f"max {limit} active")
            + f" · event #{policy['event_id']}"
        )
        budget_labels = []
        if policy.get("max_total_tokens") is not None:
            budget_labels.append(f"{policy['max_total_tokens']} tokens")
        if policy.get("max_total_cost_usd") is not None:
            budget_labels.append(f"${policy['max_total_cost_usd']:.6f}")
        if policy.get("max_attempts") is not None:
            budget_labels.append(f"{policy['max_attempts']} attempts")
        if policy.get("max_wall_clock_seconds") is not None:
            budget_labels.append(
                f"{policy['max_wall_clock_seconds']}s wall clock"
            )
        if budget_labels:
            lines.append("Workflow budgets: " + " · ".join(budget_labels))
    if value["dependencies"]:
        labels = ", ".join(
            f"{item['task_id']}={item['status']}@#{item['status_event_id']}"
            for item in value["dependencies"]
        )
        lines.append(f"Dependencies: {labels}")
    if value["deadline_at"] is not None:
        lines.append(f"Deadline: {value['deadline_at']}")
    if value["completion_summary"]:
        lines.append(f"Result: {value['completion_summary']}")
    trace = ", ".join(f"#{event_id}" for event_id in value["explanation"]["event_ids"])
    lines.append(f"Trace: {trace}")
    if value.get('next_action'):
        lines.append(f"Next: {shlex.join(value['next_action'])}")
    return "\n".join(lines)


def _format_explanation(value: dict) -> str:
    trace = ", ".join(f"#{event_id}" for event_id in value["event_ids"])
    result = f"Task {value['task_id']}: {value['summary']}\nReason: {value['code']}\nTrace: {trace}"
    if value.get('next_action'):
        result += f"\nNext: {shlex.join(value['next_action'])}"
    return result


def _format_workflow(value: dict) -> str:
    lines = [
        f"Workflow {value['correlation_id']} · {value['status']} · {value['task_count']} tasks"
    ]
    control = value.get("control", {})
    if control.get("status") != "active":
        lines.append(
            f"  Control: {control.get('status')} · event #{control.get('last_event_id')}"
        )
    policy = value.get("policy", {"status": "unmaterialized"})
    if policy.get("status") == "active":
        limit = policy.get("max_active_assignments")
        lines.append(
            "  Policy: "
            + ("unbounded concurrency" if limit is None else f"max {limit} active")
            + f" · {policy.get('source')} · event #{policy.get('event_id')}"
        )
    else:
        lines.append("  Policy: awaiting materialization by PM")
    scheduling = value.get("scheduling", {})
    if scheduling.get("policy"):
        previous = scheduling.get("last_assignment_event_id")
        lines.append(
            "  Scheduling: workflow round-robin · latest assignment "
            + (f"#{previous}" if previous is not None else "none")
        )
    budget = value.get("budget", {})
    lines.append(
        f"  Accounting: {budget.get('tokens', 0)} tokens · "
        f"${budget.get('cost_usd', 0):.6f} · "
        f"{budget.get('attempts', 0)} attempts · "
        f"{budget.get('active_reservations', 0)} active reservations"
    )
    for task in value["tasks"]:
        dependencies = [item["task_id"] for item in task["dependencies"]]
        dependency_text = f" · depends on {dependencies}" if dependencies else ""
        scheduling_text = f" · priority {task['priority']}" + (
            f" · not before {task['not_before']}"
            if task["not_before"] is not None
            else ""
        )
        lines.append(
            f"  Task {task['task_id']} · {task['effective_status']}@#{task['status_event_id']} · "
            f"{task['title']}{dependency_text}{scheduling_text}"
        )
        lines.append(f"    {task['explanation']['summary']}")
    lines.append(
        _format_usage(value["telemetry"])
    )
    telemetry_ids = value["telemetry"]["event_ids"]
    if telemetry_ids:
        lines.append(
            "Telemetry trace: " + ", ".join(f"#{event_id}" for event_id in telemetry_ids)
        )
    return "\n".join(lines)


def _format_usage(telemetry: dict) -> str:
    usage = telemetry["usage"]
    model = telemetry["model"]
    token_text = (
        f"{usage['total_tokens']} tokens"
        if telemetry["usage_samples"]
        else "tokens not reported"
    )
    cost_text = (
        f"${usage['cost_usd']:.6f}"
        if telemetry["cost_samples"]
        else "cost not reported"
    )
    return (
        f"Usage: {token_text} · {cost_text} · model calls "
        f"{model['completed']} completed/{model['failed']} failed/"
        f"{model['open']} open"
    )


def _friendly_error(exc: Exception) -> str:
    if isinstance(exc, httpx.ConnectError):
        return "the bus is not reachable; start the server or check --url"
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code == 401:
            return "authentication failed; set LOGPLANE_TOKEN or pass --token"
        return f"server returned HTTP {exc.response.status_code}"
    return str(exc)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _positive_number(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def _nonnegative_number(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative finite number")
    return parsed


def _lease_default() -> float:
    raw = os.environ.get("LOGPLANE_WORKER_LEASE_SECONDS")
    if raw is None:
        return DEFAULT_LEASE_SECONDS
    try:
        return _positive_number(raw)
    except (argparse.ArgumentTypeError, ValueError):
        return DEFAULT_LEASE_SECONDS


if __name__ == "__main__":
    raise SystemExit(main())
