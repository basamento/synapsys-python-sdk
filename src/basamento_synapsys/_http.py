"""Minimal non-redirecting HTTP transport built on the standard library."""

from __future__ import annotations

import http.client
import json
import ssl
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import SplitResult, urlsplit

from ._config import Config

MAX_RESPONSE_BYTES = 1_048_576


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: Mapping[str, Any] | None


class HttpClient:
    """HTTP client with no redirect behavior and separate connection/read timeouts."""

    def __init__(self, config: Config) -> None:
        if config.core_url is None:
            raise ValueError("core_url is required before creating HttpClient")
        self._config = config
        self._base = urlsplit(config.core_url)

    def health(self) -> HttpResponse:
        return self._request("GET", "/api/health", None, authenticated=False)

    def heartbeat(self, payload: Mapping[str, Any]) -> HttpResponse:
        return self._request("POST", "/api/v1/workers/heartbeat", payload, authenticated=True)

    def _request(
        self,
        method: str,
        endpoint: str,
        payload: Mapping[str, Any] | None,
        *,
        authenticated: bool,
    ) -> HttpResponse:
        connection = self._connection(self._base)
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        headers: dict[str, str] = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))
        if authenticated:
            headers["Authorization"] = f"Bearer {self._config.core_token or ''}"

        request_expired = threading.Event()

        def expire_request() -> None:
            request_expired.set()
            connection.close()

        request_timer: threading.Timer | None = None
        try:
            # Connecting explicitly lets the TCP/TLS phase use its own timeout.
            connection.connect()
            if connection.sock is not None:
                connection.sock.settimeout(self._config.request_timeout)
            # The socket timeout bounds one blocking I/O operation. Closing the
            # connection at the deadline additionally bounds the whole request,
            # including a peer that keeps trickling bytes just before each socket
            # timeout would expire.
            request_timer = threading.Timer(self._config.request_timeout, expire_request)
            request_timer.daemon = True
            request_timer.start()
            connection.request(method, _join_path(self._base.path, endpoint), body, headers)
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if request_expired.is_set():
                raise TimeoutError("Core request exceeded request_timeout")
            if len(raw) > MAX_RESPONSE_BYTES:
                raise ValueError("Core response exceeded 1 MiB")
            parsed: Mapping[str, Any] | None = None
            if raw:
                value = json.loads(raw.decode("utf-8"))
                if isinstance(value, dict):
                    parsed = value
            return HttpResponse(response.status, parsed)
        finally:
            if request_timer is not None:
                request_timer.cancel()
            connection.close()

    def _connection(self, parsed: SplitResult) -> http.client.HTTPConnection:
        hostname = parsed.hostname
        if hostname is None:
            raise ValueError("Core URL has no hostname")
        port = parsed.port
        if parsed.scheme == "https":
            return http.client.HTTPSConnection(
                hostname,
                port=port,
                timeout=self._config.connect_timeout,
                context=ssl.create_default_context(),
            )
        return http.client.HTTPConnection(hostname, port=port, timeout=self._config.connect_timeout)


def _join_path(base_path: str, endpoint: str) -> str:
    prefix = base_path.rstrip("/")
    suffix = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    return f"{prefix}{suffix}" or "/"
