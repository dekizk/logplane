"""Public, configuration-driven adapters for existing agent systems."""

from __future__ import annotations

import importlib
import inspect
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Mapping, Optional
from urllib.parse import urlsplit

import httpx

from logplane.executor_protocol import (
    CURRENT_PROTOCOL_VERSION,
    assignment_message,
    cancellation_message,
    parse_outcome_message,
    validate_protocol_version,
)
from logplane.limits import MAX_RETRY_AFTER_SECONDS
from logplane.executors import (
    DEFAULT_MAX_PROTOCOL_BYTES,
    AssignmentContext,
    InProcessExecutor,
    Outcome,
    PermanentFailure,
    RetryableFailure,
    SubprocessExecutor,
    outcome_from_dict,
)


INTEGRATION_CONFIG_VERSION = 1
DEFAULT_ADAPTER_TIMEOUT_SECONDS = 300.0
RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})


def _retry_after(value: Optional[str]) -> Optional[float]:
    """A ``Retry-After`` header given in seconds; the date form is ignored."""
    try:
        seconds = float(value) if value is not None else None
    except ValueError:
        return None
    if seconds is None or not math.isfinite(seconds) or seconds <= 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


class PythonAgentAdapter(InProcessExecutor):
    """Stable public name for adapting a callable or object with ``run()``."""


@dataclass(frozen=True)
class CliAgentConfig:
    command: tuple[str, ...]
    protocol_version: int = CURRENT_PROTOCOL_VERSION
    timeout_seconds: float = DEFAULT_ADAPTER_TIMEOUT_SECONDS
    max_protocol_bytes: int = DEFAULT_MAX_PROTOCOL_BYTES
    working_directory: Optional[Path] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.command, tuple)
            or not self.command
            or not all(
                isinstance(item, str) and item.strip() for item in self.command
            )
        ):
            raise ValueError("CLI adapter command must be a non-empty string tuple")
        validate_protocol_version(self.protocol_version, allow_legacy=False)
        object.__setattr__(
            self,
            "timeout_seconds",
            _positive_number(self.timeout_seconds, "CLI adapter timeout_seconds"),
        )
        _positive_int(self.max_protocol_bytes, "CLI adapter max_protocol_bytes")
        if self.working_directory is not None:
            path = Path(self.working_directory).expanduser().resolve()
            if not path.is_dir():
                raise ValueError(
                    "CLI adapter working_directory must identify an existing directory"
                )
            object.__setattr__(self, "working_directory", path)

    @classmethod
    def from_mapping(
        cls,
        value: object,
        *,
        base_directory: Optional[Path] = None,
    ) -> "CliAgentConfig":
        mapping = _mapping(value, "adapter")
        allowed = {
            "type",
            "command",
            "protocol_version",
            "timeout_seconds",
            "max_protocol_bytes",
            "working_directory",
        }
        _exact_fields(mapping, allowed, required={"type", "command"}, name="CLI adapter")
        if mapping["type"] != "cli":
            raise ValueError("CLI adapter type must be 'cli'")
        command = mapping["command"]
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(item, str) and item.strip() for item in command)
        ):
            raise ValueError("CLI adapter command must be a non-empty string array")
        protocol_version = validate_protocol_version(
            mapping.get("protocol_version", CURRENT_PROTOCOL_VERSION),
            allow_legacy=False,
        )
        timeout = _positive_number(
            mapping.get("timeout_seconds", DEFAULT_ADAPTER_TIMEOUT_SECONDS),
            "CLI adapter timeout_seconds",
        )
        max_bytes = _positive_int(
            mapping.get("max_protocol_bytes", DEFAULT_MAX_PROTOCOL_BYTES),
            "CLI adapter max_protocol_bytes",
        )
        working_directory = _optional_path(
            mapping.get("working_directory"),
            base_directory=base_directory,
            name="CLI adapter working_directory",
        )
        return cls(
            command=tuple(command),
            protocol_version=protocol_version,
            timeout_seconds=timeout,
            max_protocol_bytes=max_bytes,
            working_directory=working_directory,
        )

    def build(self) -> "CliAgentAdapter":
        return CliAgentAdapter(self)


class CliAgentAdapter(SubprocessExecutor):
    """Run a JSON-capable CLI using the selected versioned protocol."""

    def __init__(self, config: CliAgentConfig):
        if not isinstance(config, CliAgentConfig):
            raise TypeError("config must be a CliAgentConfig")
        self.config = config
        super().__init__(
            config.command,
            timeout=config.timeout_seconds,
            max_protocol_bytes=config.max_protocol_bytes,
            protocol_version=config.protocol_version,
            working_directory=config.working_directory,
        )


@dataclass(frozen=True)
class HttpAgentConfig:
    endpoint: str
    protocol_version: int = CURRENT_PROTOCOL_VERSION
    timeout_seconds: float = DEFAULT_ADAPTER_TIMEOUT_SECONDS
    max_protocol_bytes: int = DEFAULT_MAX_PROTOCOL_BYTES
    token_env: Optional[str] = None
    cancellation_endpoint: Optional[str] = None
    allow_remote: bool = False

    def __post_init__(self) -> None:
        endpoint = _nonempty_string(self.endpoint, "HTTP adapter endpoint")
        object.__setattr__(self, "endpoint", endpoint)
        validate_protocol_version(self.protocol_version, allow_legacy=False)
        object.__setattr__(
            self,
            "timeout_seconds",
            _positive_number(self.timeout_seconds, "HTTP adapter timeout_seconds"),
        )
        _positive_int(self.max_protocol_bytes, "HTTP adapter max_protocol_bytes")
        if self.token_env is not None:
            object.__setattr__(
                self,
                "token_env",
                _nonempty_string(self.token_env, "HTTP adapter token_env"),
            )
        if self.cancellation_endpoint is not None:
            object.__setattr__(
                self,
                "cancellation_endpoint",
                _nonempty_string(
                    self.cancellation_endpoint,
                    "HTTP adapter cancellation_endpoint",
                ),
            )
        if not isinstance(self.allow_remote, bool):
            raise ValueError("HTTP adapter allow_remote must be a boolean")
        _validate_http_endpoint(endpoint, allow_remote=self.allow_remote)
        if self.cancellation_endpoint is not None:
            _validate_http_endpoint(
                self.cancellation_endpoint,
                allow_remote=self.allow_remote,
            )
        any_remote = not _is_loopback(endpoint) or (
            self.cancellation_endpoint is not None
            and not _is_loopback(self.cancellation_endpoint)
        )
        if any_remote and self.token_env is None:
            raise ValueError("remote HTTP adapters require token_env")

    @classmethod
    def from_mapping(cls, value: object) -> "HttpAgentConfig":
        mapping = _mapping(value, "adapter")
        allowed = {
            "type",
            "endpoint",
            "protocol_version",
            "timeout_seconds",
            "max_protocol_bytes",
            "token_env",
            "cancellation_endpoint",
            "allow_remote",
        }
        _exact_fields(mapping, allowed, required={"type", "endpoint"}, name="HTTP adapter")
        if mapping["type"] != "http":
            raise ValueError("HTTP adapter type must be 'http'")
        endpoint = _nonempty_string(mapping["endpoint"], "HTTP adapter endpoint")
        token_env = mapping.get("token_env")
        if token_env is not None:
            token_env = _nonempty_string(token_env, "HTTP adapter token_env")
        cancellation_endpoint = mapping.get("cancellation_endpoint")
        if cancellation_endpoint is not None:
            cancellation_endpoint = _nonempty_string(
                cancellation_endpoint,
                "HTTP adapter cancellation_endpoint",
            )
        allow_remote = mapping.get("allow_remote", False)
        if not isinstance(allow_remote, bool):
            raise ValueError("HTTP adapter allow_remote must be a boolean")
        return cls(
            endpoint=endpoint,
            protocol_version=validate_protocol_version(
                mapping.get("protocol_version", CURRENT_PROTOCOL_VERSION),
                allow_legacy=False,
            ),
            timeout_seconds=_positive_number(
                mapping.get("timeout_seconds", DEFAULT_ADAPTER_TIMEOUT_SECONDS),
                "HTTP adapter timeout_seconds",
            ),
            max_protocol_bytes=_positive_int(
                mapping.get("max_protocol_bytes", DEFAULT_MAX_PROTOCOL_BYTES),
                "HTTP adapter max_protocol_bytes",
            ),
            token_env=token_env,
            cancellation_endpoint=cancellation_endpoint,
            allow_remote=allow_remote,
        )

    def build(self, *, client: Optional[httpx.Client] = None) -> "HttpAgentAdapter":
        return HttpAgentAdapter(self, client=client)


class HttpAgentAdapter:
    """Send versioned assignments to a local-first HTTP agent endpoint.

    Plain HTTP is accepted only for loopback endpoints. Remote endpoints must
    opt in explicitly, use HTTPS, and supply a bearer token through an
    environment-variable reference rather than embedding a credential in the
    configuration file.
    """

    def __init__(
        self,
        config: HttpAgentConfig,
        *,
        client: Optional[httpx.Client] = None,
    ):
        if not isinstance(config, HttpAgentConfig):
            raise TypeError("config must be an HttpAgentConfig")
        _validate_http_endpoint(config.endpoint, allow_remote=config.allow_remote)
        if config.cancellation_endpoint is not None:
            _validate_http_endpoint(
                config.cancellation_endpoint,
                allow_remote=config.allow_remote,
            )
        token = os.environ.get(config.token_env) if config.token_env else None
        remote = not _is_loopback(config.endpoint) or (
            config.cancellation_endpoint is not None
            and not _is_loopback(config.cancellation_endpoint)
        )
        if remote and not token:
            raise ValueError(
                "remote HTTP adapters require token_env naming a populated environment variable"
            )
        self.config = config
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=config.timeout_seconds)

    def execute(self, assignment: AssignmentContext) -> Outcome:
        message = assignment_message(
            assignment.to_dict(),
            self.config.protocol_version,
        )
        encoded = json.dumps(
            message,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > self.config.max_protocol_bytes:
            return PermanentFailure(
                "assignment_too_large",
                "serialized assignment exceeds the HTTP adapter protocol limit",
            )
        headers = {
            **self._headers,
            "Content-Type": "application/json",
            "Idempotency-Key": assignment.assignment_id,
            # Header names are part of the HTTP adapter protocol and keep
            # their pre-0.13 spelling so existing agents stay compatible.
            "X-Agent-Bus-Assignment-Id": assignment.assignment_id,
            "X-Agent-Bus-Effect-Scope": assignment.effect_scope,
            "X-Agent-Bus-Protocol-Version": str(self.config.protocol_version),
        }
        try:
            with self._client.stream(
                "POST",
                self.config.endpoint,
                content=encoded,
                headers=headers,
                timeout=self.config.timeout_seconds,
            ) as response:
                status_code = response.status_code
                if status_code >= 500 or status_code in RETRYABLE_HTTP_STATUSES:
                    return RetryableFailure(
                        "http_agent_unavailable",
                        f"HTTP agent returned status {status_code}",
                        retry_after_seconds=_retry_after(
                            response.headers.get("Retry-After")
                        ),
                    )
                if status_code >= 400:
                    return PermanentFailure(
                        "http_agent_rejected",
                        f"HTTP agent returned status {status_code}",
                    )
                content = bytearray()
                for chunk in response.iter_bytes():
                    if len(content) + len(chunk) > self.config.max_protocol_bytes:
                        return PermanentFailure(
                            "protocol_output_too_large",
                            "HTTP agent response exceeds the protocol limit",
                        )
                    content.extend(chunk)
        except httpx.HTTPError as exc:
            return RetryableFailure(
                "http_transport_failed",
                f"HTTP adapter request failed: {exc.__class__.__name__}",
            )
        try:
            decoded = json.loads(bytes(content).decode("utf-8"))
            return outcome_from_dict(
                parse_outcome_message(decoded, self.config.protocol_version)
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return PermanentFailure(
                "invalid_executor_output",
                f"HTTP agent returned an invalid JSON outcome: {exc}",
            )

    def cancel(self, assignment_id: str) -> None:
        if self.config.cancellation_endpoint is None:
            return
        headers = {
            **self._headers,
            "Content-Type": "application/json",
            "Idempotency-Key": f"cancel:{assignment_id}",
            "X-Agent-Bus-Assignment-Id": assignment_id,
        }
        try:
            self._client.post(
                self.config.cancellation_endpoint,
                json=cancellation_message(assignment_id),
                headers=headers,
                timeout=min(self.config.timeout_seconds, 10.0),
            )
        except httpx.HTTPError:
            # Runtime ownership fencing suppresses late results. Cancellation
            # delivery is best-effort and must not take authority itself.
            return

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


@dataclass(frozen=True)
class IntegrationConfig:
    worker_name: str
    capabilities: tuple[str, ...]
    capacity: int
    adapter: CliAgentConfig | HttpAgentConfig
    schema_version: int = INTEGRATION_CONFIG_VERSION

    @classmethod
    def from_file(cls, path: str | Path) -> "IntegrationConfig":
        config_path = Path(path).expanduser().resolve()
        try:
            value = json.loads(config_path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ValueError(f"integration config not found: {config_path}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"integration config is not valid JSON: {exc}") from exc
        mapping = _mapping(value, "integration config")
        _exact_fields(
            mapping,
            {"schema_version", "worker", "adapter"},
            required={"schema_version", "worker", "adapter"},
            name="integration config",
        )
        if mapping["schema_version"] != INTEGRATION_CONFIG_VERSION:
            raise ValueError(
                f"unsupported integration config schema_version {mapping['schema_version']!r}"
            )
        worker = _mapping(mapping["worker"], "worker")
        _exact_fields(
            worker,
            {"name", "capabilities", "capacity"},
            required={"name"},
            name="worker",
        )
        name = _nonempty_string(worker["name"], "worker name")
        raw_capabilities = worker.get("capabilities", [])
        if not isinstance(raw_capabilities, list) or not all(
            isinstance(item, str) and item.strip() for item in raw_capabilities
        ):
            raise ValueError("worker capabilities must be a string array")
        capabilities = tuple(dict.fromkeys(raw_capabilities))
        capacity = _positive_int(worker.get("capacity", 1), "worker capacity")
        adapter_mapping = _mapping(mapping["adapter"], "adapter")
        adapter_type = adapter_mapping.get("type")
        if adapter_type == "cli":
            adapter = CliAgentConfig.from_mapping(
                adapter_mapping,
                base_directory=config_path.parent,
            )
        elif adapter_type == "http":
            adapter = HttpAgentConfig.from_mapping(adapter_mapping)
        else:
            raise ValueError("adapter type must be 'cli' or 'http'")
        return cls(
            worker_name=name,
            capabilities=capabilities,
            capacity=capacity,
            adapter=adapter,
        )

    def build_executor(self):
        return self.adapter.build()


def load_python_target(reference: str, *, instantiate_classes: bool = True) -> object:
    """Load ``module:attribute`` from an installed package or current directory."""
    if not isinstance(reference, str) or reference.count(":") != 1:
        raise ValueError("Python target must use module:attribute syntax")
    module_name, attribute_name = reference.split(":", 1)
    if not module_name or not attribute_name:
        raise ValueError("Python target must use module:attribute syntax")
    # Console entry points normally put their bin directory—not the caller's
    # working directory—at sys.path[0]. Local adapter modules are an intentional
    # onboarding surface, so make the launch directory importable just as
    # ``python -c`` and ``python -m`` do.
    working_directory = Path.cwd().resolve()
    if not any(
        _path_entry_resolves_to(entry, working_directory)
        for entry in sys.path
    ):
        sys.path.insert(0, str(working_directory))
        importlib.invalidate_caches()
    try:
        module: ModuleType = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValueError(
            f"Python target module {module_name!r} is not importable; install its package "
            "in this environment or add it to Python's import path"
        ) from exc
    try:
        target = getattr(module, attribute_name)
    except AttributeError as exc:
        raise ValueError(f"Python target {reference!r} was not found") from exc
    if instantiate_classes and inspect.isclass(target):
        try:
            target = target()
        except TypeError as exc:
            raise ValueError(
                f"Python target class {reference!r} must be constructible without arguments"
            ) from exc
    return target


def _path_entry_resolves_to(entry: object, directory: Path) -> bool:
    if not isinstance(entry, str):
        return False
    try:
        return Path(entry or ".").resolve() == directory
    except OSError:
        return False


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    return value


def _exact_fields(
    value: Mapping[str, object],
    allowed: set[str],
    *,
    required: set[str],
    name: str,
) -> None:
    missing = required - set(value)
    unexpected = set(value) - allowed
    if missing:
        raise ValueError(f"{name} is missing fields: {', '.join(sorted(missing))}")
    if unexpected:
        raise ValueError(f"{name} has unexpected fields: {', '.join(sorted(unexpected))}")


def _nonempty_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_number(value: object, name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def _optional_path(
    value: object,
    *,
    base_directory: Optional[Path],
    name: str,
) -> Optional[Path]:
    if value is None:
        return None
    raw = _nonempty_string(value, name)
    path = Path(raw).expanduser()
    if not path.is_absolute() and base_directory is not None:
        path = base_directory / path
    path = path.resolve()
    if not path.is_dir():
        raise ValueError(f"{name} must identify an existing directory")
    return path


def _is_loopback(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def _validate_http_endpoint(url: str, *, allow_remote: bool) -> None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("HTTP adapter endpoint must be an absolute http(s) URL")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("HTTP adapter endpoint must not contain credentials or a fragment")
    if _is_loopback(url):
        return
    if not allow_remote:
        raise ValueError("remote HTTP adapter endpoints require allow_remote=true")
    if parsed.scheme != "https":
        raise ValueError("remote HTTP adapter endpoints must use HTTPS")
