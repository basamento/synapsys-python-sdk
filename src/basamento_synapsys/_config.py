"""Strict configuration resolution for the framework-neutral SDK."""

from __future__ import annotations

import ipaddress
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from ._duration import parse_duration, parse_whole_seconds
from .errors import SynapsysConfigError

_OPTION_NAMES = (
    "enabled",
    "core_url",
    "core_token",
    "worker_name",
    "host",
    "heartbeat_interval",
    "fail_fast",
    "connect_timeout",
    "request_timeout",
    "capture_console",
    "log_level",
    "logger",
)

_ENV_NAMES = {
    "enabled": "SYNAPSYS_ENABLED",
    "core_url": "SYNAPSYS_CORE_URL",
    "core_token": "SYNAPSYS_CORE_TOKEN",
    "worker_name": "SYNAPSYS_WORKER_NAME",
    "host": "SYNAPSYS_HOST",
    "heartbeat_interval": "SYNAPSYS_HEARTBEAT_INTERVAL",
    "fail_fast": "SYNAPSYS_FAIL_FAST",
    "connect_timeout": "SYNAPSYS_CONNECT_TIMEOUT",
    "request_timeout": "SYNAPSYS_REQUEST_TIMEOUT",
    "capture_console": "SYNAPSYS_CAPTURE_CONSOLE",
    "log_level": "SYNAPSYS_LOG_LEVEL",
}


@dataclass(frozen=True)
class Config:
    enabled: bool
    core_url: str | None
    core_token: str | None
    worker_name: str | None
    host: str | None
    heartbeat_interval: int
    fail_fast: bool
    connect_timeout: float
    request_timeout: float
    capture_console: bool
    log_level: str
    logger: Any

    def public(self) -> Mapping[str, Any]:
        return {
            "enabled": self.enabled,
            "core_url": self.core_url,
            "worker_name": self.worker_name,
            "host": self.host,
            "heartbeat_interval": self.heartbeat_interval,
            "fail_fast": self.fail_fast,
            "connect_timeout": self.connect_timeout,
            "request_timeout": self.request_timeout,
            "capture_console": self.capture_console,
            "log_level": self.log_level,
        }


def resolve_config(options: Mapping[str, Any], env: Mapping[str, str] | None = None) -> Config:
    environment = os.environ if env is None else env
    unknown = sorted(set(options) - set(_OPTION_NAMES))
    if unknown:
        passed = ", ".join(repr(key) for key in unknown)
        expected = ", ".join(repr(key) for key in _OPTION_NAMES)
        raise SynapsysConfigError(
            f"[Synapsys] Unknown Synapsys options: {passed}. Expected any of: {expected}."
        )

    def raw(name: str, default: Any) -> Any:
        if name in options:
            return options[name]
        env_name = _ENV_NAMES.get(name)
        if env_name is not None and environment.get(env_name, "").strip() != "":
            return environment[env_name]
        return default

    host = _optional_string(raw("host", None), "host")
    if host is None:
        try:
            host = socket.gethostname() or None
        except OSError:
            host = None

    return Config(
        enabled=_boolean(raw("enabled", True), "enabled"),
        core_url=_optional_string(raw("core_url", None), "core_url"),
        core_token=_optional_string(raw("core_token", None), "core_token"),
        worker_name=_optional_string(raw("worker_name", None), "worker_name"),
        host=host,
        heartbeat_interval=parse_whole_seconds(
            raw("heartbeat_interval", "5s"), "heartbeat_interval"
        ),
        fail_fast=_boolean(raw("fail_fast", False), "fail_fast"),
        connect_timeout=parse_duration(raw("connect_timeout", "2s"), "connect_timeout"),
        request_timeout=parse_duration(raw("request_timeout", "5s"), "request_timeout"),
        capture_console=_boolean(raw("capture_console", True), "capture_console"),
        log_level=_log_level(raw("log_level", "info")),
        logger=raw("logger", None),
    )


def validate_for_startup(config: Config) -> tuple[str, ...]:
    if config.core_url is None:
        raise SynapsysConfigError(
            "[Synapsys] core_url is required. Set it directly or with SYNAPSYS_CORE_URL."
        )
    if config.worker_name is None:
        raise SynapsysConfigError(
            "[Synapsys] worker_name is required. Set it directly or with SYNAPSYS_WORKER_NAME."
        )

    parsed = urlsplit(config.core_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise SynapsysConfigError(
            "[Synapsys] core_url must be an absolute http:// or https:// URL."
        )
    if parsed.username is not None or parsed.password is not None:
        raise SynapsysConfigError("[Synapsys] core_url must not contain credentials.")
    if parsed.query or parsed.fragment:
        raise SynapsysConfigError("[Synapsys] core_url must not contain a query or fragment.")

    warnings = []
    if config.core_token is None:
        warnings.append("Core token is not configured | Core will reject heartbeats")
    elif parsed.scheme == "http" and not _is_loopback(parsed.hostname):
        warnings.append("Core URL uses HTTP while a token is configured | bearer token is exposed")
    return tuple(warnings)


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise SynapsysConfigError(f"[Synapsys] {name} must be true or false, got {value!r}.")


def _optional_string(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise SynapsysConfigError(f"[Synapsys] {name} must be a string, got {value!r}.")
    stripped = value.strip()
    return stripped or None


def _log_level(value: Any) -> str:
    if not isinstance(value, str) or value.strip().lower() not in {
        "debug",
        "info",
        "warning",
        "error",
        "silent",
    }:
        raise SynapsysConfigError(
            "[Synapsys] log_level must be debug, info, warning, error or silent."
        )
    return value.strip().lower()


def _is_loopback(hostname: str) -> bool:
    if hostname.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False
