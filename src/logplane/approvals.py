"""In-attempt approval of individual tool calls.

A running attempt can ask a human before one tool call and keep running while
it waits. Nothing here changes task state: the attempt keeps its assignment,
the PM never replays these events, and the answer reaches only the call that
asked. Three things can settle a call:

- a standing ``deny`` rule for the workflow (checked first),
- a standing ``allow`` rule for the workflow,
- otherwise a human, through ``logplane approve`` or ``logplane deny``.

Every outcome is an event. The bus refuses a resolution, or a rule set,
recorded under the requesting worker's own actor, and the gate checks the same
thing again when it reads the answer. Anything the gate cannot establish
(the bus is unreachable, nobody answers in time, the answer is malformed) is a
denial.

Rules match text; they are not a sandbox. A rule's ``tool`` and ``input`` are
shell-style patterns (``fnmatch``) over the tool name and a one-line summary of
its input: the command for Bash, the path for file tools, the URL for WebFetch.
An allow rule is literal and never matches a Bash command that contains shell
control characters, so ``Bash:git status*`` cannot wave through
``git status; rm -rf .``; such commands go to a human. A deny rule is read more
broadly: it is tried against each part of a compound command, and with git's
global options removed, so ``Bash:git push*`` also denies
``git -C repo push``.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import re
import shlex
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

import httpx

from logplane.limits import MAX_APPROVAL_SUMMARY_CHARS
from logplane.topics import APPROVAL_TOPICS

ALLOW = "allow"
DENY = "deny"
DEFAULT_WAIT_SECONDS = 3600.0
DEFAULT_POLL_SECONDS = 1.0

_SHELL_TOOLS = frozenset({"Bash"})
_SHELL_CONTROL = re.compile(r"[;&|`<>\n\r]|\$\(|\$\{")
_SHELL_SEGMENTS = re.compile(r"[;&|\n\r]+")
_GIT_OPTIONS_WITH_VALUE = frozenset({"-C", "-c", "--git-dir", "--work-tree", "--namespace",
                                     "--exec-path", "--super-prefix", "--config-env"})
_PATH_FIELDS = ("file_path", "notebook_path", "path")
_SUMMARY_FIELDS = {"Bash": ("command",), "WebFetch": ("url",), "WebSearch": ("query",)}

__all__ = [
    "ALLOW", "DENY", "APPROVAL_TOPICS", "ApprovalGate", "ApprovalResult",
    "ApprovalRules", "Rule", "approvals_view", "evaluate_rules", "input_digest",
    "latest_rules", "parse_rule", "summarize_tool_input",
]


@dataclass(frozen=True)
class Rule:
    tool: str
    input: Optional[str] = None

    def to_dict(self) -> dict:
        value = {"tool": self.tool}
        if self.input is not None:
            value["input"] = self.input
        return value

    def text(self) -> str:
        return self.tool if self.input is None else f"{self.tool}:{self.input}"


@dataclass(frozen=True)
class ApprovalRules:
    event_id: int
    actor: str
    allow: tuple[Rule, ...] = ()
    deny: tuple[Rule, ...] = ()


@dataclass(frozen=True)
class ApprovalResult:
    decision: str
    reason: str
    # "rule", "human", "timeout" or "error"
    source: str
    event_id: Optional[int] = None

    @property
    def allowed(self) -> bool:
        return self.decision == ALLOW


def parse_rule(text: str) -> Rule:
    """Parse ``TOOL`` or ``TOOL:INPUT_PATTERN`` as typed on the command line."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("an approval rule must not be empty")
    tool, separator, pattern = text.strip().partition(":")
    if not tool.strip() or (separator and not pattern.strip()):
        raise ValueError(f"approval rule {text!r} must be TOOL or TOOL:INPUT_PATTERN")
    return Rule(tool.strip(), pattern.strip() if separator else None)


def input_digest(tool_input: object) -> str:
    """A stable digest of the whole tool input, so a summary can be checked."""
    encoded = json.dumps(tool_input, sort_keys=True, separators=(",", ":"),
                         allow_nan=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def summarize_tool_input(tool_name: str, tool_input: object) -> str:
    """The one line a rule matches and a human reads; not yet truncated."""
    if isinstance(tool_input, Mapping):
        for field in _SUMMARY_FIELDS.get(tool_name, ()) + _PATH_FIELDS:
            value = tool_input.get(field)
            if isinstance(value, str) and value:
                return value
    if tool_input in (None, {}, ""):
        return ""
    return json.dumps(tool_input, sort_keys=True, separators=(",", ":"), default=str)


def _plain_git(command: str) -> str:
    """``git -C dir -c k=v push`` as ``git push``: the form a deny rule names."""
    try:
        words = shlex.split(command)
    except ValueError:
        words = command.split()
    while words and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", words[0]):
        words = words[1:]  # leading VAR=value assignments
    if not words or words[0] != "git":
        return command
    rest = words[1:]
    while rest and rest[0].startswith("-"):
        option = rest[0]
        rest = rest[2:] if option in _GIT_OPTIONS_WITH_VALUE else rest[1:]
    return " ".join(["git", *rest])


def _deny_forms(summary: str) -> list[str]:
    """Every way a shell command is offered to a deny pattern.

    The whole command, each part of a compound command, and each part with
    git's global options removed, so ``git push*`` also catches
    ``cd repo && git -C . push``. Best effort: this reads text, not a shell.
    """
    forms = [summary]
    for part in _SHELL_SEGMENTS.split(summary):
        part = part.strip()
        if not part:
            continue
        for form in (part, _plain_git(part)):
            if form not in forms:
                forms.append(form)
    return forms


def _matches(rule: Rule, tool_name: str, summary: str, *, allowing: bool) -> bool:
    if not fnmatch.fnmatchcase(tool_name, rule.tool):
        return False
    if rule.input is None:
        return True
    if tool_name not in _SHELL_TOOLS:
        return fnmatch.fnmatchcase(summary, rule.input)
    if allowing:
        # Allow rules stay literal: the whole command, and never a compound one.
        return (not _SHELL_CONTROL.search(summary)
                and fnmatch.fnmatchcase(summary, rule.input))
    return any(fnmatch.fnmatchcase(form, rule.input) for form in _deny_forms(summary))


def evaluate_rules(
    rules: Optional[ApprovalRules], tool_name: str, summary: str
) -> Optional[tuple[str, Rule]]:
    """Deny rules win, then allow rules; ``None`` means ask a human."""
    if rules is None:
        return None
    for rule in rules.deny:
        if _matches(rule, tool_name, summary, allowing=False):
            return DENY, rule
    for rule in rules.allow:
        if _matches(rule, tool_name, summary, allowing=True):
            return ALLOW, rule
    return None


def _rule_list(value: object) -> Optional[tuple[Rule, ...]]:
    if not isinstance(value, list):
        return None
    rules = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("tool"), str):
            return None
        pattern = item.get("input")
        if pattern is not None and not isinstance(pattern, str):
            return None
        rules.append(Rule(item["tool"], pattern))
    return tuple(rules)


def latest_rules(
    events: Iterable[Mapping[str, Any]], *, exclude_actor: Optional[str] = None
) -> Optional[ApprovalRules]:
    """The newest well-formed rule set, ignoring any set by ``exclude_actor``.

    Each ``approval.rules_set`` replaces the workflow's rules as a whole.
    """
    latest: Optional[ApprovalRules] = None
    for event in events:
        if event.get("topic") != "approval.rules_set":
            continue
        actor, event_id, payload = event.get("actor"), event.get("id"), event.get("payload")
        if (
            not isinstance(actor, str)
            or actor == exclude_actor
            or type(event_id) is not int
            or not isinstance(payload, dict)
        ):
            continue
        allow, deny = _rule_list(payload.get("allow")), _rule_list(payload.get("deny"))
        if allow is None or deny is None:
            continue
        if latest is None or event_id > latest.event_id:
            latest = ApprovalRules(event_id, actor, allow, deny)
    return latest


def approvals_view(
    events: Iterable[Mapping[str, Any]],
    *,
    active_assignments: Optional[Mapping[str, Any]] = None,
    now: Optional[float] = None,
) -> list[dict]:
    """One row per human-facing request, oldest first, from approval events.

    ``active_assignments`` maps the assignment ids that still own their task to
    anything; when given, an unanswered request whose attempt has ended is
    reported as ``expired`` instead of ``pending``.
    """
    rows: dict[int, dict] = {}
    for event in events:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        if event.get("topic") == "approval.requested":
            rows[event["id"]] = {
                "request_event_id": event["id"],
                "task_id": payload.get("task_id"),
                "assignment_id": payload.get("assignment_id"),
                "correlation_id": event.get("correlation_id"),
                "agent": event.get("actor"),
                "tool_name": payload.get("tool_name"),
                "summary": payload.get("summary"),
                "input_digest": payload.get("input_digest"),
                "requested_at": event.get("ts"),
                "status": "pending",
            }
        elif event.get("topic") == "approval.resolved":
            row = rows.get(event.get("caused_by"))
            if (
                row is None
                or row["status"] != "pending"
                or event.get("actor") == row["agent"]
                or payload.get("decision") not in (ALLOW, DENY)
            ):
                continue
            row.update(
                status="allowed" if payload["decision"] == ALLOW else "denied",
                resolved_event_id=event.get("id"),
                resolved_by=event.get("actor"),
            )
            if isinstance(payload.get("reason"), str):
                row["reason"] = payload["reason"]
    result = []
    for row in sorted(rows.values(), key=lambda item: item["request_event_id"]):
        if row["status"] == "pending":
            if (
                active_assignments is not None
                and row["assignment_id"] not in active_assignments
            ):
                row["status"] = "expired"
            elif now is not None and isinstance(row["requested_at"], (int, float)):
                row["waiting_seconds"] = max(0.0, round(now - row["requested_at"], 1))
        result.append(row)
    return result


class ApprovalGate:
    """Ask before one tool call of one running attempt.

    ``bus`` is the worker's own ``BusClient`` (its actor is the worker name).
    ``check`` blocks until a rule or a human settles the call, or until
    ``wait_seconds`` pass, and never raises for a bus failure: it denies.
    """

    def __init__(
        self,
        bus,
        *,
        task_id: int,
        assignment_id: str,
        assignment_event_id: int,
        worker_instance_id: str,
        correlation_id: Optional[str],
        wait_seconds: float = DEFAULT_WAIT_SECONDS,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        should_stop: Optional[Callable[[], bool]] = None,
    ):
        if wait_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("wait_seconds and poll_seconds must be positive")
        self.bus = bus
        self.task_id = task_id
        self.assignment_id = assignment_id
        self.assignment_event_id = assignment_event_id
        self.worker_instance_id = worker_instance_id
        self.correlation_id = correlation_id
        self.wait_seconds = wait_seconds
        self.poll_seconds = poll_seconds
        self._clock, self._sleep, self._should_stop = clock, sleep, should_stop

    @classmethod
    def for_assignment(cls, bus, assignment, **options) -> "ApprovalGate":
        return cls(
            bus,
            task_id=assignment.task_id,
            assignment_id=assignment.assignment_id,
            assignment_event_id=assignment.assignment_event_id,
            worker_instance_id=assignment.worker_instance_id,
            correlation_id=assignment.correlation_id,
            **options,
        )

    def check(
        self, tool_name: str, tool_input: object = None, *, call_id: Optional[str] = None
    ) -> ApprovalResult:
        if not isinstance(tool_name, str) or not tool_name.strip():
            return ApprovalResult(DENY, "the tool call has no tool name", "error")
        try:
            return self._check(tool_name.strip(), tool_input, call_id)
        except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
            return ApprovalResult(
                DENY, f"approval could not be obtained ({type(exc).__name__})", "error"
            )

    def _check(self, tool_name: str, tool_input: object, call_id: Optional[str]) -> ApprovalResult:
        summary = summarize_tool_input(tool_name, tool_input)
        call = {
            "task_id": self.task_id,
            "assignment_id": self.assignment_id,
            "worker_instance_id": self.worker_instance_id,
            "approval_id": f"{self.assignment_id}:{call_id or uuid.uuid4().hex}",
            "tool_name": tool_name,
            "summary": _truncate(summary),
            "input_digest": input_digest(tool_input),
        }
        rules = self._rules()
        matched = evaluate_rules(rules, tool_name, summary)
        if matched is not None and rules is not None:
            decision, rule = matched
            event = self.bus.publish(
                "approval.rule_applied",
                {**call, "decision": decision, "rules_event_id": rules.event_id,
                 "rule": rule.text()},
                caused_by=rules.event_id,
                idempotency_key=f"approval-rule:{call['approval_id']}",
            )
            return ApprovalResult(
                decision, f"standing {decision} rule {rule.text()!r} "
                f"(rules event #{rules.event_id})", "rule", event.get("id"))
        request = self.bus.publish(
            "approval.requested", call, caused_by=self.assignment_event_id,
            idempotency_key=f"approval:{call['approval_id']}",
        )
        return self._wait(request, call["approval_id"])

    def _rules(self) -> Optional[ApprovalRules]:
        if self.correlation_id is None:
            return None
        events = self.bus.query_all(
            topics=["approval.rules_set"], correlation_id=self.correlation_id
        )
        return latest_rules(events, exclude_actor=self.bus.actor)

    def _wait(self, request: Mapping[str, Any], approval_id: str) -> ApprovalResult:
        request_id = request["id"]
        cursor = request_id
        deadline = self._clock() + self.wait_seconds
        while True:
            events = self.bus.query(
                after_id=cursor, topics=["approval.resolved"],
                correlation_id=request.get("correlation_id"),
            )
            for event in events:
                cursor = max(cursor, event["id"])
                payload = event.get("payload")
                if (
                    event.get("caused_by") != request_id
                    or not isinstance(payload, dict)
                    or payload.get("approval_id") != approval_id
                    or event.get("actor") == self.bus.actor
                    or payload.get("decision") not in (ALLOW, DENY)
                ):
                    continue
                reason = payload.get("reason")
                detail = f": {reason}" if isinstance(reason, str) and reason.strip() else ""
                verdict = "approved" if payload["decision"] == ALLOW else "denied"
                return ApprovalResult(
                    payload["decision"],
                    f"{verdict} by {event.get('actor')} (event #{event['id']}){detail}",
                    "human", event["id"])
            if self._should_stop is not None and self._should_stop():
                return ApprovalResult(DENY, "the attempt was stopped while waiting", "error",
                                      request_id)
            if self._clock() >= deadline:
                return ApprovalResult(
                    DENY, f"nobody answered approval request #{request_id} within "
                    f"{self.wait_seconds:g} seconds", "timeout", request_id)
            self._sleep(self.poll_seconds)


def _truncate(summary: str) -> str:
    if len(summary) <= MAX_APPROVAL_SUMMARY_CHARS:
        return summary
    return summary[: MAX_APPROVAL_SUMMARY_CHARS - 1] + "…"
