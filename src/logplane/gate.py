"""Hook entry points that put an agent's tool calls behind ``ApprovalGate``.

``logplane gate claude-hook`` is a Claude Code ``PreToolUse`` command hook: it
reads the pending tool call on standard input, settles it through the bus, and
prints the permission decision Claude Code acts on. The worker that starts the
agent supplies the attempt through ``LOGPLANE_GATE_*`` variables
(``gate_environment``). Whatever goes wrong, the answer is a denial.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
import time
from pathlib import Path
from typing import Mapping, Optional, TextIO

from logplane.approvals import DEFAULT_WAIT_SECONDS, DENY, ApprovalGate, ApprovalResult

ENVIRONMENT_PREFIX = "LOGPLANE_GATE_"
WAITING_DIRECTORY = ENVIRONMENT_PREFIX + "WAITING_DIR"
_WAITING_GRACE_SECONDS = 10.0
_REQUIRED = ("URL", "ACTOR", "TASK_ID", "ASSIGNMENT_ID", "ASSIGNMENT_EVENT_ID",
             "WORKER_INSTANCE_ID")


def gate_environment(assignment, *, bus_url: str, actor: str,
                     wait_seconds: float = DEFAULT_WAIT_SECONDS) -> dict[str, str]:
    """Variables that tell a gate hook which attempt it is guarding."""
    values = {
        "URL": bus_url,
        "ACTOR": actor,
        "TASK_ID": str(assignment.task_id),
        "ASSIGNMENT_ID": assignment.assignment_id,
        "ASSIGNMENT_EVENT_ID": str(assignment.assignment_event_id),
        "WORKER_INSTANCE_ID": assignment.worker_instance_id,
        "WAIT_SECONDS": f"{wait_seconds:g}",
    }
    if assignment.correlation_id is not None:
        values["CORRELATION_ID"] = assignment.correlation_id
    return {ENVIRONMENT_PREFIX + name: value for name, value in values.items()}


def gate_from_environment(environment: Mapping[str, str], *, bus_factory=None) -> ApprovalGate:
    values = {name: environment.get(ENVIRONMENT_PREFIX + name, "") for name in _REQUIRED}
    missing = [ENVIRONMENT_PREFIX + name for name, value in values.items() if not value]
    if missing:
        raise ValueError("missing " + ", ".join(missing))
    wait_seconds = float(environment.get(ENVIRONMENT_PREFIX + "WAIT_SECONDS")
                         or DEFAULT_WAIT_SECONDS)
    if not math.isfinite(wait_seconds) or wait_seconds <= 0:
        raise ValueError(ENVIRONMENT_PREFIX + "WAIT_SECONDS must be positive")
    if bus_factory is None:
        from logplane.client import BusClient

        def bus_factory(url: str, actor: str):
            # The hook runs in the agent's working directory; keep the
            # client's offset directory out of it.
            offsets = Path(tempfile.gettempdir()) / "logplane-gate-offsets"
            return BusClient(url, actor=actor, offset_dir=offsets)
    return ApprovalGate(
        bus_factory(values["URL"], values["ACTOR"]),
        task_id=int(values["TASK_ID"]),
        assignment_id=values["ASSIGNMENT_ID"],
        assignment_event_id=int(values["ASSIGNMENT_EVENT_ID"]),
        worker_instance_id=values["WORKER_INSTANCE_ID"],
        correlation_id=environment.get(ENVIRONMENT_PREFIX + "CORRELATION_ID") or None,
        wait_seconds=wait_seconds,
    )


def approval_pending(directory: Path, *, now: Optional[float] = None) -> bool:
    """Whether a gate hook is waiting right now, for the worker that started it.

    A hook leaves a marker in ``directory`` while it waits. Each marker holds
    the time after which it no longer counts, so one left behind by a killed
    hook cannot hold a run open.
    """
    now = time.time() if now is None else now
    try:
        entries = list(os.scandir(directory))
    except OSError:
        return False
    for entry in entries:
        try:
            if float(Path(entry.path).read_text()) > now:
                return True
        except (OSError, ValueError):
            continue
    return False


def claude_hook_response(result: ApprovalResult) -> dict:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": result.decision,
            "permissionDecisionReason": f"logplane: {result.reason}",
        }
    }


def run_claude_hook(stdin: TextIO, stdout: TextIO, environment: Mapping[str, str], *,
                    gate: Optional[ApprovalGate] = None) -> int:
    """Settle one PreToolUse call. Always prints a decision and returns 0."""
    marker: Optional[Path] = None
    try:
        call = json.loads(stdin.read())
        if not isinstance(call, dict) or not isinstance(call.get("tool_name"), str):
            raise ValueError("hook input has no tool_name")
        gate = gate or gate_from_environment(environment)
        call_id = call.get("tool_use_id")
        waiting = environment.get(WAITING_DIRECTORY)
        if waiting:
            marker = Path(waiting) / f"{os.getpid()}.wait"
            marker.write_text(str(time.time() + gate.wait_seconds + _WAITING_GRACE_SECONDS))
        result = gate.check(call["tool_name"], call.get("tool_input"),
                            call_id=call_id if isinstance(call_id, str) and call_id else None)
    except Exception as exc:  # a hook that crashes must still deny
        result = ApprovalResult(DENY, f"the approval gate failed ({type(exc).__name__}: {exc})",
                                "error")
    finally:
        if marker is not None:
            try:
                marker.unlink()
            except OSError:
                pass
    print(json.dumps(claude_hook_response(result)), file=stdout, flush=True)
    return 0
