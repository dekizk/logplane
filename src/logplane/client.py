"""Blocking client library for logplane."""

import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Iterator, Optional

import httpx

CURRENT_SCHEMA_VERSION = 2
_UNSET = object()


class BusProtocolError(RuntimeError):
    """The bus returned a malformed event stream frame."""


class BusClient:
    def __init__(
        self,
        base_url: str,
        actor: str,
        offset_dir: Path = Path(".offsets"),
        offset_name: Optional[str] = None,
        token: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.actor = actor
        token = token if token is not None else os.environ.get("LOGPLANE_TOKEN")
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        offset_dir.mkdir(parents=True, exist_ok=True)
        consumer_name = offset_name or actor
        if re.fullmatch(r"[A-Za-z0-9_.-]+", consumer_name) and consumer_name not in {".", ".."}:
            safe_name = consumer_name
        else:
            prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", consumer_name).strip("_")[:40]
            digest = hashlib.sha256(consumer_name.encode()).hexdigest()[:12]
            safe_name = f"{prefix or 'consumer'}-{digest}"
        self.offset_file = offset_dir / f"{safe_name}.offset"
        self.offset_lock_file = offset_dir / f"{safe_name}.offset.lock"

    def database_identity(self) -> str:
        response = httpx.get(f"{self.base_url}/health", headers=self._headers, timeout=10)
        response.raise_for_status()
        body = response.json()
        value = body.get("database_id") if isinstance(body, dict) else None
        if not isinstance(value, str) or not value:
            raise BusProtocolError("bus identity unavailable; disable PM snapshots or upgrade the server")
        return value

    def publish(
        self,
        topic: str,
        payload: dict,
        caused_by: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        schema_version: int = CURRENT_SCHEMA_VERSION,
        correlation_id: Optional[str] = None,
        producer: Optional[dict] = None,
    ) -> dict:
        body = {
            "topic": topic,
            "actor": self.actor,
            "payload": payload,
            "caused_by": caused_by,
            "schema_version": schema_version,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        if correlation_id is not None:
            body["correlation_id"] = correlation_id
        if producer is not None:
            body["producer"] = producer
        response = httpx.post(
            f"{self.base_url}/events", json=body, headers=self._headers, timeout=10
        )
        response.raise_for_status()
        return response.json()

    def cancel_task(
        self,
        task_id: int,
        *,
        reason: str = "cancelled by requester",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            raise ValueError("task_id must be a positive integer")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        return self.publish(
            "task.cancel_requested",
            {"task_id": task_id, "reason": reason.strip()},
            idempotency_key=idempotency_key or f"cancel:task:{task_id}",
        )

    def pause_task(
        self,
        task_id: int,
        *,
        reason: str = "paused by requester",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Request a pause. A task can be paused and resumed many times, so the
        default key is unique per call; pass ``idempotency_key`` to make a retry
        of the same request safe."""
        self._validate_control_target(task_id, reason)
        return self.publish(
            "task.pause_requested",
            {"task_id": task_id, "reason": reason.strip()},
            idempotency_key=idempotency_key or f"pause:{uuid.uuid4().hex}",
        )

    def resume_task(
        self,
        task_id: int,
        *,
        reason: str = "resumed by requester",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Request a resume; see ``pause_task`` for the default key."""
        self._validate_control_target(task_id, reason)
        return self.publish(
            "task.resume_requested",
            {"task_id": task_id, "reason": reason.strip()},
            idempotency_key=idempotency_key or f"resume:{uuid.uuid4().hex}",
        )

    def pause_workflow(
        self,
        correlation_id: str,
        *,
        reason: str = "workflow paused by requester",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        return self._workflow_control(
            "workflow.pause_requested",
            correlation_id,
            reason,
            idempotency_key,
        )

    def resume_workflow(
        self,
        correlation_id: str,
        *,
        reason: str = "workflow resumed by requester",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        return self._workflow_control(
            "workflow.resume_requested",
            correlation_id,
            reason,
            idempotency_key,
        )

    def set_workflow_policy(
        self,
        correlation_id: str,
        *,
        max_active_assignments: object = _UNSET,
        max_total_tokens: object = _UNSET,
        max_total_cost_usd: object = _UNSET,
        max_attempts: object = _UNSET,
        max_wall_clock_seconds: object = _UNSET,
        reserve_tokens_per_assignment: object = _UNSET,
        reserve_cost_usd_per_assignment: object = _UNSET,
        reason: str,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise ValueError("correlation_id must be a non-empty string")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        payload = {"source": "operator", "reason": reason.strip()}
        integer_limits = {
            "max_active_assignments": max_active_assignments,
            "max_total_tokens": max_total_tokens,
            "max_attempts": max_attempts,
        }
        number_limits = {
            "max_total_cost_usd": max_total_cost_usd,
            "max_wall_clock_seconds": max_wall_clock_seconds,
        }
        for name, value in integer_limits.items():
            if value is _UNSET:
                continue
            if value is not None and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                raise ValueError(f"{name} must be null or positive")
            payload[name] = value
        for name, value in number_limits.items():
            if value is _UNSET:
                continue
            if value is not None and (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be null or positive")
            payload[name] = value
        for name, value, integer in (
            (
                "reserve_tokens_per_assignment",
                reserve_tokens_per_assignment,
                True,
            ),
            (
                "reserve_cost_usd_per_assignment",
                reserve_cost_usd_per_assignment,
                False,
            ),
        ):
            if value is _UNSET:
                continue
            valid_type = (
                isinstance(value, int)
                if integer
                else isinstance(value, (int, float))
            )
            if not valid_type or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be non-negative")
            if not integer and not math.isfinite(value):
                raise ValueError(f"{name} must be non-negative")
            payload[name] = value
        if set(payload) == {"source", "reason"}:
            raise ValueError("at least one workflow policy field is required")
        token_fields = {
            "max_total_tokens",
            "reserve_tokens_per_assignment",
        }
        if token_fields.intersection(payload) and not token_fields.issubset(payload):
            raise ValueError("token budget and reservation must be changed together")
        if payload.get("max_total_tokens") is not None and not (
            0 < payload["reserve_tokens_per_assignment"]
            <= payload["max_total_tokens"]
        ):
            raise ValueError(
                "finite token budget requires a positive reservation no larger than the budget"
            )
        cost_fields = {
            "max_total_cost_usd",
            "reserve_cost_usd_per_assignment",
        }
        if cost_fields.intersection(payload) and not cost_fields.issubset(payload):
            raise ValueError("cost budget and reservation must be changed together")
        if payload.get("max_total_cost_usd") is not None and not (
            0 < payload["reserve_cost_usd_per_assignment"]
            <= payload["max_total_cost_usd"]
        ):
            raise ValueError(
                "finite cost budget requires a positive reservation no larger than the budget"
            )
        return self.publish(
            "workflow.policy_set",
            payload,
            correlation_id=correlation_id.strip(),
            idempotency_key=idempotency_key or f"workflow-policy:{uuid.uuid4().hex}",
        )

    def set_agent_policy(
        self,
        agent_name: str,
        *,
        max_active_assignments: Optional[int],
        reason: str,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        if not isinstance(agent_name, str) or not agent_name.strip():
            raise ValueError("agent_name must be a non-empty string")
        if max_active_assignments is not None and (
            not isinstance(max_active_assignments, int)
            or isinstance(max_active_assignments, bool)
            or max_active_assignments <= 0
        ):
            raise ValueError("max_active_assignments must be null or positive")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        return self.publish(
            "agent.policy_set",
            {
                "agent_name": agent_name.strip(),
                "source": "operator",
                "reason": reason.strip(),
                "max_active_assignments": max_active_assignments,
            },
            idempotency_key=idempotency_key or f"agent-policy:{uuid.uuid4().hex}",
        )

    def supersede_task(
        self,
        task_id: int,
        replacement: dict,
        *,
        reason: str,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        self._validate_control_target(task_id, reason)
        if not isinstance(replacement, dict):
            raise ValueError("replacement must be a JSON object")
        payload = dict(replacement)
        payload["supersedes_task_id"] = task_id
        payload["supersession_reason"] = reason.strip()
        return self.publish(
            "task.created",
            payload,
            idempotency_key=idempotency_key or f"supersede:{uuid.uuid4().hex}",
        )

    @staticmethod
    def _validate_control_target(task_id: int, reason: str) -> None:
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            raise ValueError("task_id must be a positive integer")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")

    def _workflow_control(
        self,
        topic: str,
        correlation_id: str,
        reason: str,
        idempotency_key: Optional[str],
    ) -> dict:
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise ValueError("correlation_id must be a non-empty string")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        return self.publish(
            topic,
            {"reason": reason.strip()},
            correlation_id=correlation_id.strip(),
            idempotency_key=idempotency_key or f"workflow-control:{uuid.uuid4().hex}",
        )

    def subscribe(
        self,
        topics: Optional[list[str]] = None,
        from_id: int = 0,
        on_idle: Optional[Callable[[], None]] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
        on_reconnect: Optional[Callable[[], None]] = None,
        stream_timeout: Optional[float] = None,
    ) -> Iterator[dict]:
        """Follow SSE with reconnect, resuming after the last yielded event.

        on_idle runs on server keepalives. Coordinators can use it for
        time-based reconciliation without introducing a second state-mutating
        thread.

        The delay before reconnecting doubles after each failed stream, up to
        30 seconds, and returns to one second once the bus is heard from
        again (an event or a keepalive).

        on_reconnect runs once for each stream after the first, just before
        that stream's first event is yielded or its first on_idle runs: the
        moment the caller is about to look at the log again after a break.
        """
        last_id = from_id
        backoff = 1.0
        streams = 0
        while stop_event is None or not stop_event.is_set():
            yielded = False
            streams += 1
            pending = [on_reconnect if streams > 1 else None]

            def resumed(pending=pending) -> None:
                callback, pending[0] = pending[0], None
                if callback is not None:
                    callback()

            idle = on_idle
            if on_idle is not None and pending[0] is not None:
                def idle(resumed=resumed, on_idle=on_idle) -> None:
                    resumed()
                    on_idle()

            def heard(idle=idle) -> None:
                # A keepalive proves the bus is there as well as an event
                # does, so the next reconnect starts from the short delay.
                nonlocal backoff
                backoff = 1.0
                if idle is not None:
                    idle()

            try:
                for event in self._stream_once(
                    topics,
                    last_id,
                    heard,
                    correlation_id,
                    stop_event,
                    **({"stream_timeout": stream_timeout} if stream_timeout is not None else {}),
                ):
                    resumed()
                    yielded = True
                    last_id = event["id"]
                    backoff = 1.0
                    yield event
                # A graceful server close is still a disconnect. Avoid a tight
                # reconnect loop if a proxy repeatedly returns an empty stream.
                if not yielded:
                    if stop_event is not None:
                        if stop_event.wait(backoff):
                            return
                    else:
                        time.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
            except httpx.HTTPError as exc:
                print(
                    f"[{self.actor}] bus connection lost ({exc.__class__.__name__}); "
                    f"reconnecting from #{last_id} in {backoff:.0f}s",
                    flush=True,
                )
                if stop_event is not None:
                    if stop_event.wait(backoff):
                        return
                else:
                    time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _stream_once(
        self,
        topics: Optional[list[str]],
        from_id: int,
        on_idle: Optional[Callable[[], None]] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
        stream_timeout: Optional[float] = None,
    ) -> Iterator[dict]:
        params: dict = {"from_id": from_id}
        if topics:
            params["topics"] = ",".join(topics)
        if correlation_id is not None:
            params["correlation_id"] = correlation_id
        with httpx.stream(
            "GET",
            f"{self.base_url}/events/stream",
            params=params,
            headers=self._headers,
            timeout=stream_timeout,
        ) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if stop_event is not None and stop_event.is_set():
                    return
                if line.startswith("data: "):
                    raw = line[len("data: ") :]
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise BusProtocolError(
                            "bus stream contained invalid JSON"
                        ) from exc
                    if not isinstance(event, dict):
                        raise BusProtocolError("bus stream event must be an object")
                    event_id = event.get("id")
                    if (
                        not isinstance(event_id, int)
                        or isinstance(event_id, bool)
                        or event_id <= 0
                    ):
                        raise BusProtocolError(
                            "bus stream event must contain a positive integer id"
                        )
                    yield event
                elif line.startswith(":") and on_idle is not None:
                    on_idle()

    def query(
        self,
        after_id: int = 0,
        topics: Optional[list[str]] = None,
        limit: int = 1000,
        correlation_id: Optional[str] = None,
    ) -> list[dict]:
        params: dict = {"after_id": after_id, "limit": limit}
        if topics:
            params["topics"] = ",".join(topics)
        if correlation_id is not None:
            params["correlation_id"] = correlation_id
        response = httpx.get(
            f"{self.base_url}/events", params=params, headers=self._headers, timeout=10
        )
        response.raise_for_status()
        return response.json()

    def get_event(self, event_id: int) -> dict:
        if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id <= 0:
            raise ValueError("event_id must be a positive integer")
        response = httpx.get(
            f"{self.base_url}/events/{event_id}",
            headers=self._headers,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def query_all(
        self,
        after_id: int = 0,
        topics: Optional[list[str]] = None,
        page_size: int = 1000,
        correlation_id: Optional[str] = None,
    ) -> list[dict]:
        """Read a complete snapshot using bounded pages."""
        events: list[dict] = []
        cursor = after_id
        while True:
            page = self.query(
                after_id=cursor,
                topics=topics,
                limit=page_size,
                correlation_id=correlation_id,
            )
            if not page:
                return events
            events.extend(page)
            cursor = page[-1]["id"]
            if len(page) < page_size:
                return events

    def iter_events(
        self, *, after_id: int = 0, through_id: int,
        topics: Optional[list[str]] = None, page_size: int = 1000,
    ) -> Iterator[dict]:
        """Yield an immutable prefix in bounded pages, never chase a moving head.

        Gaps between IDs are normal with topic filtering. The caller must check
        that its required terminal event was present; an empty page is EOF, not
        proof that an arbitrary requested event exists.
        """
        if type(after_id) is not int or type(through_id) is not int or not 0 <= after_id <= through_id:
            raise ValueError("history bounds must be ordered nonnegative integers")
        if type(page_size) is not int or not 1 <= page_size <= 10000:
            raise ValueError("page_size must be between 1 and 10000")
        cursor = after_id
        while cursor < through_id:
            page = self.query(after_id=cursor, topics=topics, limit=page_size)
            if not isinstance(page, list) or len(page) > page_size:
                raise BusProtocolError("history response is not a bounded event page")
            if not page:
                return
            for event in page:
                if not isinstance(event, dict):
                    raise BusProtocolError("history page contains a non-event")
                event_id = event.get("id")
                if type(event_id) is not int or event_id <= cursor:
                    raise BusProtocolError("history is not strictly ordered")
                if event_id > through_id:
                    return
                if not isinstance(event.get("payload"), dict) or not isinstance(event.get("topic"), str):
                    raise BusProtocolError("history contains a malformed event")
                if topics and event["topic"] not in topics:
                    raise BusProtocolError("history contains an event outside the requested topics")
                cursor = event_id
                yield event
                if cursor == through_id:
                    return
            if len(page) < page_size:
                return

    def load_offset(self) -> int:
        try:
            return int(self.offset_file.read_text())
        except (FileNotFoundError, ValueError):
            return 0

    def save_offset(self, event_id: int) -> None:
        """Atomically and monotonically persist a consumer resume point."""
        if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id < 0:
            raise ValueError("event_id must be a non-negative integer")

        with self.offset_lock_file.open("a+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            if event_id <= self.load_offset():
                return

            fd, temp_name = tempfile.mkstemp(
                prefix=f".{self.offset_file.name}.",
                dir=self.offset_file.parent,
                text=True,
            )
            try:
                with os.fdopen(fd, "w") as temp_file:
                    temp_file.write(str(event_id))
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                os.replace(temp_name, self.offset_file)
            finally:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass
