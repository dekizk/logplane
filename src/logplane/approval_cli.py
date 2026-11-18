"""The operator commands for tool-call approvals and standing rules."""

from __future__ import annotations

import json
import shlex
import uuid
from typing import TextIO

import httpx

from logplane.approvals import (
    ALLOW, DENY, approvals_view, latest_rules, parse_rule,
)
from logplane.projection import ACTIVE_TASK_STATUSES
from logplane.topics import APPROVAL_TOPICS

COMMANDS = frozenset({"approvals", "approve", "deny", "approval-rules"})


def add_parsers(commands, *, add_common_options, positive_int) -> None:
    listing = commands.add_parser(
        "approvals", help="list tool-call approval requests waiting for an answer")
    listing.add_argument("--workflow", help="filter by correlation ID")
    listing.add_argument("--all", action="store_true",
                         help="include answered and expired requests")
    listing.add_argument("--config", help="local config path; otherwise use normal URL resolution")
    add_common_options(listing)

    for name in ("approve", "deny"):
        control = commands.add_parser(
            name, help=f"{name} one waiting tool call; the agent keeps running")
        control.add_argument("request_event", type=positive_int,
                             help="event ID of the approval.requested event")
        control.add_argument("--reason", required=name == "deny",
                             help="recorded with the answer and shown to the agent")
        control.add_argument("--config", help="local config path; otherwise use normal URL resolution")
        control.add_argument("--idempotency-key", help="reuse unchanged when retrying this command")
        add_common_options(control)

    rules = commands.add_parser(
        "approval-rules",
        help="show or replace a workflow's standing tool-call rules",
        description="Without --allow, --deny or --clear, show the current rules. Each "
                    "change replaces the workflow's whole rule set. RULE is TOOL or "
                    "TOOL:INPUT_PATTERN with shell-style wildcards, for example "
                    "'Read' or 'Bash:git status*'. Deny rules win over allow rules; a "
                    "call no rule matches goes to a human.")
    rules.add_argument("correlation_id")
    rules.add_argument("--allow", action="append", default=[], metavar="RULE")
    rules.add_argument("--deny", action="append", default=[], metavar="RULE")
    rules.add_argument("--clear", action="store_true", help="remove every rule")
    rules.add_argument("--reason", help="required when changing the rules")
    rules.add_argument("--config", help="local config path; otherwise use normal URL resolution")
    rules.add_argument("--idempotency-key", help="reuse unchanged when retrying this command")
    add_common_options(rules)


def run(args, client, *, stdout: TextIO, stderr: TextIO, clock, render) -> int:
    try:
        if args.command == "approvals":
            return _list(args, client, stdout=stdout, clock=clock, render=render)
        if args.command in {"approve", "deny"}:
            return _resolve(args, client, stdout=stdout, stderr=stderr, render=render)
        return _rules(args, client, stdout=stdout, render=render)
    except httpx.HTTPStatusError as exc:
        # The bus explains refusals (already answered, wrong actor) in `detail`.
        if exc.response.status_code not in (409, 422):
            raise
        try:
            detail = exc.response.json().get("detail")
        except ValueError:
            detail = None
        print(f"logplane: the bus refused this: {detail or 'invalid request'}", file=stderr)
        return 4


def _publish(args, intent: dict) -> dict:
    headers = {"Authorization": f"Bearer {args.token}"} if args.token else {}
    response = httpx.post(f"{args.url}/events", json=intent, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()


def _list(args, client, *, stdout, clock, render) -> int:
    from logplane.operator_controls import read_snapshot

    state, head, _, _, _ = read_snapshot(client)
    events = list(client.iter_events(through_id=head, topics=sorted(APPROVAL_TOPICS)))
    active = {
        task.assignment_id: task.task_id for task in state.tasks.values()
        if task.assignment_id is not None and task.status in ACTIVE_TASK_STATUSES
    }
    rows = approvals_view(events, active_assignments=active, now=clock())
    if args.workflow:
        rows = [row for row in rows if row["correlation_id"] == args.workflow]
    if not args.all:
        rows = [row for row in rows if row["status"] == "pending"]
    for row in rows:
        if row["status"] == "pending":
            row["next_actions"] = [
                ["logplane", "approve", str(row["request_event_id"])],
                ["logplane", "deny", str(row["request_event_id"]), "--reason", "REASON"],
            ]
    value = {"approvals": rows, "observed_through_id": head}
    render(value, args, stdout, _format_list(rows, show_all=args.all))
    return 0


def _format_list(rows: list[dict], *, show_all: bool) -> str:
    if not rows:
        return "No tool-call approvals " + ("recorded." if show_all else "are waiting.")
    lines = []
    for row in rows:
        waiting = (f" · waiting {row['waiting_seconds']:g}s"
                   if "waiting_seconds" in row else "")
        lines.append(
            f"#{row['request_event_id']} {row['status']} · task {row['task_id']} · "
            f"{row['agent']} wants {row['tool_name']}{waiting}")
        if row["summary"]:
            lines.append(f"    {row['summary']}")
        for action in row.get("next_actions", []):
            lines.append(f"    {shlex.join(action)}")
    return "\n".join(lines)


def _resolve(args, client, *, stdout, stderr, render) -> int:
    found = client.query(after_id=args.request_event - 1, limit=1)
    request = found[0] if found else None
    if (
        request is None
        or request.get("id") != args.request_event
        or request.get("topic") != "approval.requested"
    ):
        raise ValueError(
            f"event #{args.request_event} is not an approval request; "
            "run `logplane approvals` for the waiting ones")
    requested = request["payload"]
    from logplane.operator_controls import read_snapshot

    state = read_snapshot(client)[0]
    task = state.tasks.get(requested["task_id"])
    if (
        task is None
        or task.assignment_id != requested["assignment_id"]
        or task.status not in ACTIVE_TASK_STATUSES
    ):
        print(f"logplane: approval request #{args.request_event} has expired: its attempt "
              "has ended, so an answer would reach nobody. Nothing was recorded.",
              file=stderr)
        return 4
    decision = ALLOW if args.command == "approve" else DENY
    payload = {
        "task_id": requested["task_id"],
        "assignment_id": requested["assignment_id"],
        "approval_id": requested["approval_id"],
        "decision": decision,
    }
    reason = (args.reason or "").strip()
    if args.reason is not None and not reason:
        raise ValueError("--reason must not be empty")
    if reason:
        payload["reason"] = reason
    event = _publish(args, {
        "topic": "approval.resolved", "actor": "human", "payload": payload,
        "caused_by": args.request_event, "schema_version": 2,
        "idempotency_key": args.idempotency_key
        or f"operator:approval:{args.request_event}",
    })
    verdict = "Approved" if decision == ALLOW else "Denied"
    render(event, args, stdout,
           f"{verdict} {requested['tool_name']} for task {requested['task_id']} · "
           f"event #{event['id']}\nThe agent continues with this answer if its attempt "
           "is still running.")
    return 0


def _rules(args, client, *, stdout, render) -> int:
    changing = bool(args.allow or args.deny or args.clear)
    if not changing:
        events = client.query_all(topics=["approval.rules_set"],
                                  correlation_id=args.correlation_id)
        rules = latest_rules(events)
        value = {
            "correlation_id": args.correlation_id,
            "rules_event_id": rules.event_id if rules else None,
            "set_by": rules.actor if rules else None,
            "allow": [rule.to_dict() for rule in rules.allow] if rules else [],
            "deny": [rule.to_dict() for rule in rules.deny] if rules else [],
        }
        if rules is None or not (rules.allow or rules.deny):
            text = (f"Workflow {args.correlation_id} has no standing rules; "
                    "every gated tool call goes to a human.")
        else:
            text = "\n".join(
                [f"Workflow {args.correlation_id} rules · event #{rules.event_id} "
                 f"by {rules.actor}"]
                + [f"  deny  {rule.text()}" for rule in rules.deny]
                + [f"  allow {rule.text()}" for rule in rules.allow])
        render(value, args, stdout, text)
        return 0
    if args.clear and (args.allow or args.deny):
        raise ValueError("--clear cannot be combined with --allow or --deny")
    if not (args.reason or "").strip():
        raise ValueError("--reason is required when changing the rules")
    payload = {
        "allow": [parse_rule(text).to_dict() for text in args.allow],
        "deny": [parse_rule(text).to_dict() for text in args.deny],
        "reason": args.reason.strip(),
    }
    json.dumps(payload, allow_nan=False)
    event = _publish(args, {
        "topic": "approval.rules_set", "actor": "human", "payload": payload,
        "correlation_id": args.correlation_id, "schema_version": 2,
        "idempotency_key": args.idempotency_key or f"approval-rules:{uuid.uuid4().hex}",
    })
    render(event, args, stdout,
           f"Workflow {args.correlation_id} rules replaced "
           f"({len(payload['deny'])} deny, {len(payload['allow'])} allow) · "
           f"event #{event['id']}")
    return 0
