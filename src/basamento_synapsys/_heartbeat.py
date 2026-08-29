"""Unkillable outbound heartbeat loop and command application."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from typing import Any

from ._capture import LogCapture
from ._config import Config
from ._http import HttpClient
from ._log import SynapsysLogger
from ._process import ManagedProcess, _Command

FRAMEWORK = "python"
MAX_LOG_ENTRIES_PER_BEAT = 100
MAX_LOG_BYTES_PER_BEAT = 96_000


class HeartbeatLoop:
    def __init__(
        self,
        *,
        config: Config,
        processes: Sequence[ManagedProcess],
        capture: LogCapture | None,
        logger: SynapsysLogger,
        client: HttpClient,
    ) -> None:
        self._config = config
        self._processes = list(processes)
        self._capture = capture
        self._logger = logger
        self._client = client
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._core_available = True

    def check_health(self) -> bool:
        try:
            response = self._client.health()
            reachable = 200 <= response.status < 300
        except BaseException as error:
            self._logger.debug("Core health check failed", error)
            reachable = False
        self._core_available = reachable
        return reachable

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="synapsys-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        self._thread = None

    def tick(self) -> None:
        logs: list[dict[str, Any]] = []
        if self._capture is not None:
            logs = self._capture.drain(MAX_LOG_ENTRIES_PER_BEAT, MAX_LOG_BYTES_PER_BEAT)

        payload: dict[str, Any] = {
            "workerName": self._config.worker_name,
            "framework": FRAMEWORK,
            "heartbeatInterval": self._config.heartbeat_interval,
            "processes": [process.to_heartbeat_entry() for process in self._processes],
            "logs": logs,
        }
        if self._config.host is not None:
            payload["host"] = self._config.host

        try:
            response = self._client.heartbeat(payload)
        except BaseException as error:
            self._requeue(logs)
            self._mark_unavailable("Failed to contact Core", error)
            return

        if response.status < 200 or response.status >= 300:
            self._requeue(logs)
            self._mark_unavailable(
                f"Heartbeat rejected by Core with status {response.status}", None
            )
            return

        self._mark_available()
        self._apply_commands(response.body)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.tick()
            except BaseException as error:
                self._logger.error(
                    "Unexpected heartbeat failure | heartbeat retries will continue", error
                )
            self._stop_event.wait(self._config.heartbeat_interval)

    def _apply_commands(self, body: Mapping[str, Any] | None) -> None:
        if body is None:
            return
        updates = body.get("processes")
        if not isinstance(updates, list):
            return
        by_name = {process.name: process for process in self._processes}
        for update in updates:
            if not isinstance(update, dict) or not isinstance(update.get("name"), str):
                continue
            name = update["name"]
            process = by_name.get(name)
            if process is None:
                self._logger.warning(f"Core sent a command for unknown process '{name}' | ignoring")
                continue
            try:
                self._apply(process, update)
            except BaseException as error:
                process.mark_failed()
                self._logger.error(f"Command handling failed for process '{name}'", error)

    def _apply(self, process: ManagedProcess, update: Mapping[str, Any]) -> None:
        decision = process.accept_command(
            update.get("desiredState"), update.get("desiredToken"), update.get("runId")
        )
        if decision.command is _Command.START:
            self._logger.info(
                f"Starting process '{process.name}' (type={process.type.value}, "
                f"token={decision.token}, runId={decision.run_id})"
            )
            process.handle_start(decision.run_id)
        elif decision.command is _Command.STOP:
            self._logger.info(
                f"Stopping process '{process.name}' (type={process.type.value}, "
                f"token={decision.token})"
            )
            process.handle_stop()
        process.acknowledge(decision.token)

    def _requeue(self, logs: list[dict[str, Any]]) -> None:
        if self._capture is not None:
            self._capture.requeue_front(logs)

    def _mark_unavailable(self, message: str, cause: BaseException | None) -> None:
        if self._core_available:
            self._core_available = False
            self._logger.warning(message + " | heartbeat retries will continue", cause)
        else:
            self._logger.debug(message, cause)

    def _mark_available(self) -> None:
        if not self._core_available:
            self._core_available = True
            self._logger.info("Core connection restored")
