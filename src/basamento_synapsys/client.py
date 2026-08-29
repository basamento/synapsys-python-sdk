"""Public framework-neutral Synapsys worker API."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, TypeVar, overload

from ._capture import LogCapture
from ._config import resolve_config, validate_for_startup
from ._heartbeat import HeartbeatLoop
from ._http import HttpClient
from ._log import SynapsysLogger
from ._process import EndlessProcess, ManagedProcess, ProgressiveProcess
from .errors import SynapsysConfigError, SynapsysStartupError

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class RegisteredProcess:
    name: str
    type: str
    current_state: str
    ack_token: int
    run_id: int | None


class Synapsys:
    """Register process functions and connect their lifecycle to Synapsys Core."""

    def __init__(self, **options: Any) -> None:
        self._config = resolve_config(options)
        self._logger = SynapsysLogger(self._config.logger, self._config.log_level)
        self._processes: Mapping[str, ManagedProcess] = {}
        self._capture: LogCapture | None = None
        self._client: HttpClient | None = None
        self._heartbeat: HeartbeatLoop | None = None
        self._asyncio_loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._stopping = False
        self._lock = threading.RLock()

    @property
    def config(self) -> Mapping[str, Any]:
        """Resolved non-secret configuration."""
        return MappingProxyType(dict(self._config.public()))

    @property
    def processes(self) -> tuple[RegisteredProcess, ...]:
        return tuple(
            RegisteredProcess(
                process.name,
                process.type.value,
                process.current_state,
                process.ack_token,
                process.run_id,
            )
            for process in self._processes.values()
        )

    @property
    def is_started(self) -> bool:
        with self._lock:
            return self._started

    @overload
    def endless(
        self,
        function: F,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> F: ...

    @overload
    def endless(
        self,
        function: None = None,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> Callable[[F], F]: ...

    def endless(
        self,
        function: F | None = None,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> F | Callable[[F], F]:
        """Decorate a function whose invocation runs until it is stopped."""
        return self._decorate(EndlessProcess, function, name, on_stop)

    @overload
    def progressive(
        self,
        function: F,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> F: ...

    @overload
    def progressive(
        self,
        function: None = None,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> Callable[[F], F]: ...

    def progressive(
        self,
        function: F | None = None,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> F | Callable[[F], F]:
        """Decorate a function whose invocation runs once to completion."""
        return self._decorate(ProgressiveProcess, function, name, on_stop)

    def register_endless(
        self,
        function: F,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> F:
        return self._register(EndlessProcess, function, name, on_stop)

    def register_progressive(
        self,
        function: F,
        *,
        name: str | None = None,
        on_stop: Callable[..., Any] | None = None,
    ) -> F:
        return self._register(ProgressiveProcess, function, name, on_stop)

    def start(self) -> Synapsys:
        """Validate, probe Core, install capture, and start heartbeats."""
        with self._lock:
            if not self._config.enabled:
                return self
            if self._started:
                return self
            warnings = validate_for_startup(self._config)
            for warning in warnings:
                self._logger.warning(warning)

            client = HttpClient(self._config)
            heartbeat = HeartbeatLoop(
                config=self._config,
                processes=list(self._processes.values()),
                capture=None,
                logger=self._logger,
                client=client,
            )
            reachable = heartbeat.check_health()
            if not reachable and self._config.fail_fast:
                raise SynapsysStartupError(
                    f"[Synapsys] Core is unreachable at startup: {self._config.core_url}"
                )
            if not reachable:
                self._logger.warning(
                    f"Core is unreachable at {self._config.core_url} | "
                    "heartbeat retries will continue"
                )

            capture = None
            if self._config.capture_console:
                capture = LogCapture()
                capture.install()
                for process in self._processes.values():
                    process._capture = capture
                heartbeat._capture = capture

            self._client = client
            self._heartbeat = heartbeat
            self._capture = capture
            self._started = True
            self._logger.info(
                f"Worker '{self._config.worker_name}' registered {len(self._processes)} "
                f"process(es) | core={self._config.core_url} "
                f"host={self._config.host or '<unset>'} "
                f"heartbeat={self._config.heartbeat_interval}s"
            )
            heartbeat.start()
            return self

    async def start_async(self) -> Synapsys:
        """Non-blocking lifecycle entry point for future async framework adapters."""
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._started:
                if self._asyncio_loop is None:
                    raise SynapsysConfigError(
                        "[Synapsys] A synchronously started worker cannot switch to an "
                        "application event loop while running."
                    )
                return self
            self._asyncio_loop = loop
            for process in self._processes.values():
                process.set_asyncio_loop(loop)
        try:
            return await loop.run_in_executor(None, self.start)
        except BaseException:
            with self._lock:
                self._asyncio_loop = None
                for process in self._processes.values():
                    process.set_asyncio_loop(None)
            raise

    def stop(self) -> Synapsys:
        """Stop heartbeats and processes; idempotent and non-throwing."""
        with self._lock:
            if self._stopping:
                return self
            self._stopping = True
            heartbeat = self._heartbeat
            capture = self._capture
            was_started = self._started

        try:
            if heartbeat is not None:
                heartbeat.stop(self._config.request_timeout + 1.0)
            for process in self._processes.values():
                try:
                    process.handle_stop()
                except BaseException as error:
                    self._logger.error(
                        f"Could not stop process '{process.name}' during worker shutdown", error
                    )

            deadline = time.monotonic() + self._config.request_timeout
            for process in self._processes.values():
                remaining = max(0.0, deadline - time.monotonic())
                if not process.settle(remaining):
                    self._logger.warning(
                        f"Process '{process.name}' did not stop before the shutdown timeout"
                    )
            for process in self._processes.values():
                process.set_asyncio_loop(None)
        except BaseException as error:
            self._logger.error("Unexpected worker shutdown failure", error)
        finally:
            if capture is not None:
                capture.uninstall()
            with self._lock:
                self._capture = None
                self._heartbeat = None
                self._client = None
                self._asyncio_loop = None
                self._started = False
                self._stopping = False
            if was_started:
                self._logger.info("Worker stopped")
        return self

    async def stop_async(self) -> Synapsys:
        """Non-blocking lifecycle exit point for future async framework adapters."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.stop)

    def __enter__(self) -> Synapsys:
        return self.start()

    def __exit__(self, *_args: Any) -> None:
        self.stop()

    async def __aenter__(self) -> Synapsys:
        return await self.start_async()

    async def __aexit__(self, *_args: Any) -> None:
        await self.stop_async()

    def _decorate(
        self,
        process_class: Callable[..., ManagedProcess],
        function: F | None,
        name: str | None,
        on_stop: Callable[..., Any] | None,
    ) -> F | Callable[[F], F]:
        if function is not None:
            return self._register(process_class, function, name, on_stop)

        def decorator(decorated: F) -> F:
            return self._register(process_class, decorated, name, on_stop)

        return decorator

    def _register(
        self,
        process_class: Callable[..., ManagedProcess],
        function: F,
        name: str | None,
        on_stop: Callable[..., Any] | None,
    ) -> F:
        with self._lock:
            if self._started:
                raise SynapsysConfigError(
                    "[Synapsys] Processes must be registered before the worker starts."
                )
            process_name = _resolve_name(function, name)
            if process_name in self._processes:
                raise SynapsysConfigError(
                    f"[Synapsys] Duplicate process name '{process_name}' in this worker."
                )
            process = process_class(
                name=process_name,
                body=function,
                on_stop=on_stop,
                logger=self._logger,
            )
            mutable = dict(self._processes)
            mutable[process_name] = process
            self._processes = mutable
            self._logger.debug(f"Registered {process.type.value} process '{process_name}'")
            return function


def _resolve_name(function: Callable[..., Any], explicit: str | None) -> str:
    name = explicit.strip() if isinstance(explicit, str) else ""
    if explicit is not None and not name:
        raise SynapsysConfigError("[Synapsys] Process name must not be blank.")
    if not name:
        derived = getattr(function, "__name__", "")
        if isinstance(derived, str) and derived not in {"", "<lambda>"}:
            name = derived
    if not name:
        raise SynapsysConfigError(
            "[Synapsys] Process name is required when it cannot be derived from the callable."
        )
    return name
