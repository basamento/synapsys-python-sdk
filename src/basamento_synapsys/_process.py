"""Thread-isolated process state machines."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import threading
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from ._capture import LogCapture
from ._log import SynapsysLogger
from ._stop import StopSignal
from .errors import SynapsysConfigError

ProcessCallable = Callable[..., Any]


class ProcessType(str, Enum):
    ENDLESS = "endless"
    PROGRESSIVE = "progressive"


class ProcessState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"


class _Command(str, Enum):
    NONE = "none"
    START = "start"
    STOP = "stop"


class _Outcome(str, Enum):
    """What a command handler did with the command it was given.

    ``DEFERRED`` means the command was understood but cannot be applied yet: a start
    arriving while the previous execution is still stopping. The caller must leave
    the token unacknowledged so Core redelivers it on a later beat. Acknowledging a
    deferred start as a no-op loses that run forever.
    """

    HANDLED = "handled"
    DEFERRED = "deferred"


@dataclass(frozen=True)
class _CommandDecision:
    command: _Command
    token: int
    run_id: int | None


class ManagedProcess:
    def __init__(
        self,
        *,
        name: str,
        process_type: ProcessType,
        body: ProcessCallable,
        on_stop: ProcessCallable | None,
        logger: SynapsysLogger,
    ) -> None:
        self.name = name
        self.type = process_type
        self._body = body
        self._body_takes_signal = _validate_callable(body, "process body")
        self._on_stop = on_stop
        if on_stop is not None and _validate_callable(on_stop, "on_stop"):
            raise SynapsysConfigError("[Synapsys] on_stop must accept no arguments.")
        self._logger = logger
        self._capture: LogCapture | None = None
        self._lock = threading.RLock()
        self._state = ProcessState.IDLE
        self._desired_state = ProcessState.IDLE.value
        self._desired_token = 0
        self._ack_token = 0
        self._run_id: int | None = None
        self._signal: StopSignal | None = None
        self._thread: threading.Thread | None = None
        self._cleanup_thread: threading.Thread | None = None
        self._cleanup_done: threading.Event | None = None
        self._asyncio_loop: asyncio.AbstractEventLoop | None = None

    @property
    def current_state(self) -> str:
        with self._lock:
            return self._state.value

    @property
    def ack_token(self) -> int:
        with self._lock:
            return self._ack_token

    @property
    def run_id(self) -> int | None:
        with self._lock:
            return self._run_id

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def to_heartbeat_entry(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "type": self.type.value,
                "currentState": self._state.value,
                "ackToken": self._ack_token,
                "runId": self._run_id,
            }

    def accept_command(
        self, desired_state: Any, desired_token: Any, run_id: Any
    ) -> _CommandDecision:
        with self._lock:
            incoming = run_id if isinstance(run_id, int) and not isinstance(run_id, bool) else None

            if not isinstance(desired_token, int) or isinstance(desired_token, bool):
                self._sync_run_id(incoming)
                return _CommandDecision(_Command.NONE, self._ack_token, None)
            if desired_token <= self._ack_token:
                # The run id describes the active execution, not just a new command,
                # so it is tracked even once the token has been acknowledged.
                self._sync_run_id(incoming)
                return _CommandDecision(_Command.NONE, self._ack_token, None)

            self._desired_token = desired_token
            self._desired_state = str(desired_state)
            if desired_state == ProcessState.RUNNING.value:
                # Deliberately *not* adopted here. A start that arrives while the
                # previous run is still stopping is deferred, and replacing the
                # active run's id would attach its remaining output to an execution
                # that has not begun. handle_start adopts it on acceptance.
                return _CommandDecision(_Command.START, desired_token, incoming)
            self._sync_run_id(incoming)
            if desired_state == ProcessState.STOPPING.value:
                return _CommandDecision(_Command.STOP, desired_token, None)
            if desired_state != ProcessState.IDLE.value:
                self._logger.debug(
                    f"Ignoring unsupported desired state {desired_state!r} for process "
                    f"'{self.name}'"
                )
            return _CommandDecision(_Command.NONE, desired_token, None)

    def _sync_run_id(self, run_id: int | None) -> None:
        """Track the run id Core reports for the active execution. Caller holds the lock."""
        self._run_id = run_id

    def acknowledge(self, token: int) -> None:
        with self._lock:
            if token > self._ack_token:
                self._ack_token = token

    def mark_failed(self) -> None:
        with self._lock:
            self._state = ProcessState.FAILED

    def set_asyncio_loop(self, loop: asyncio.AbstractEventLoop | None) -> None:
        """Bind async bodies to a framework/application event loop."""
        with self._lock:
            self._asyncio_loop = loop

    def handle_start(self, run_id: int | None) -> _Outcome:
        """Start the body, or report that the command has to wait.

        Returns ``HANDLED`` when the process is running (or already was), and
        ``DEFERRED`` when the previous execution has not finished. A deferred
        command is not acknowledged, so Core redelivers it.
        """
        with self._lock:
            # A duplicate start for the execution already running is an idempotent
            # no-op, and must not silently reassign the active run.
            if self._state is ProcessState.RUNNING:
                return _Outcome.HANDLED
            # Stopping, or a body that ignored cancellation and is still alive: the
            # replacement execution waits rather than overlapping it.
            if self._state is ProcessState.STOPPING or self._in_flight():
                return _Outcome.DEFERRED

            signal = StopSignal(self.name)
            self._signal = signal
            self._cleanup_done = None
            self._cleanup_thread = None
            self._run_id = run_id
            # RUNNING before the thread starts: setting it inside the thread leaves a
            # window in which a second start observes IDLE and spawns a duplicate.
            self._state = ProcessState.RUNNING
            thread = threading.Thread(
                target=self._run_body,
                args=(run_id, signal),
                name=f"synapsys-{self.type.value}-{self.name}",
                daemon=True,
            )
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                self._thread = None
                self._state = ProcessState.FAILED
                raise
            return _Outcome.HANDLED

    def handle_stop(self) -> _Outcome:
        """Request a cooperative stop. A stop for an inactive process is a no-op."""
        with self._lock:
            if self._state is not ProcessState.RUNNING:
                return _Outcome.HANDLED
            self._state = ProcessState.STOPPING
            signal = self._signal
            # Claimed here, while the lock is held, so a body returning at this very
            # moment cannot observe "no cleanup outstanding" and let a replacement
            # execution start over the top of a hook that is about to run.
            start_cleanup = (
                self.type is ProcessType.ENDLESS
                and self._on_stop is not None
                and self._cleanup_done is None
            )
            if start_cleanup:
                # An unset event, claimed under the lock, is what makes the hook
                # visible to _in_flight() and joinable before the thread exists.
                self._cleanup_done = threading.Event()
        if signal is not None:
            signal._request()
        if start_cleanup:
            self._spawn_cleanup()
        return _Outcome.HANDLED

    def _in_flight(self) -> bool:
        """Whether a body or its cleanup hook has not returned. Caller holds the lock."""
        if self._cleanup_done is not None and not self._cleanup_done.is_set():
            return True
        for candidate in (self._thread, self._cleanup_thread):
            if candidate is not None and candidate.is_alive():
                return True
        return False

    @property
    def in_flight(self) -> bool:
        with self._lock:
            return self._in_flight()

    def settle(self, timeout: float | None = None) -> bool:
        """Wait for the body and its cleanup hook. True when nothing is left running."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            thread = self._thread
            cleanup_thread = self._cleanup_thread
            cleanup_done = self._cleanup_done
        for candidate in (thread, cleanup_thread):
            if candidate is None or candidate is threading.current_thread():
                continue
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            candidate.join(remaining)
        if cleanup_done is not None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            cleanup_done.wait(remaining)
        # Re-read rather than trusting the snapshot: a hook claimed while this call
        # was waiting on the body is still work in flight.
        return not self.in_flight

    def _run_body(self, run_id: int | None, signal: StopSignal) -> None:
        failed = False
        capture = self._capture
        scope = capture.scope(self.name, run_id) if capture is not None else _null_scope()
        with scope:
            try:
                result = self._body(signal) if self._body_takes_signal else self._body()
                if inspect.isawaitable(result):
                    _run_awaitable(result, signal, self._asyncio_loop)
            except BaseException as error:
                if not signal._is_cancellation(error):
                    failed = True
                    with self._lock:
                        self._state = ProcessState.FAILED
                    if capture is not None:
                        capture.capture_exception(error)
                    self._logger.error(
                        f"{self.type.value.capitalize()} process '{self.name}' failed", error
                    )
            finally:
                if self.type is ProcessType.PROGRESSIVE and signal.requested:
                    self._invoke_on_stop_safely()

        # The cleanup thread an endless stop started must settle before this process
        # can accept a replacement execution.
        with self._lock:
            cleanup_done = self._cleanup_done
        if cleanup_done is not None:
            cleanup_done.wait()

        with self._lock:
            if not failed:
                self._state = ProcessState.IDLE
            if self._signal is signal:
                self._signal = None
            if self._thread is threading.current_thread():
                self._thread = None

    def _invoke_on_stop_safely(self) -> None:
        if self._on_stop is None:
            return
        try:
            result = self._on_stop()
            if inspect.isawaitable(result):
                _run_awaitable(result, None, self._asyncio_loop)
        except BaseException as error:
            self._logger.error(f"on_stop() failed for process '{self.name}'", error)

    def _spawn_cleanup(self) -> None:
        """Run the cleanup hook off the heartbeat thread.

        A slow ``on_stop`` must not delay the commands the same beat carries for the
        other processes, so it never runs inline on the caller's thread.
        """
        thread = threading.Thread(
            target=self._run_cleanup,
            name=f"synapsys-cleanup-{self.name}",
            daemon=True,
        )
        try:
            thread.start()
        except BaseException as error:
            with self._lock:
                done = self._cleanup_done
                self._cleanup_thread = None
            if done is not None:
                done.set()
            self._logger.error(f"Could not run on_stop() for process '{self.name}'", error)
            return
        with self._lock:
            self._cleanup_thread = thread

    def _run_cleanup(self) -> None:
        try:
            self._invoke_on_stop_safely()
        finally:
            with self._lock:
                if self._cleanup_thread is threading.current_thread():
                    self._cleanup_thread = None
                done = self._cleanup_done
            if done is not None:
                done.set()


class EndlessProcess(ManagedProcess):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(process_type=ProcessType.ENDLESS, **kwargs)


class ProgressiveProcess(ManagedProcess):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(process_type=ProcessType.PROGRESSIVE, **kwargs)


def _validate_callable(function: ProcessCallable, description: str) -> bool:
    if not callable(function):
        raise SynapsysConfigError(f"[Synapsys] {description} must be callable.")
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError) as error:
        raise SynapsysConfigError(
            f"[Synapsys] Cannot inspect {description} {function!r}."
        ) from error

    parameters = list(signature.parameters.values())
    if any(
        parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        for parameter in parameters
    ):
        raise SynapsysConfigError(
            f"[Synapsys] {description} must accept zero arguments or one StopSignal."
        )
    if not parameters:
        return False
    if len(parameters) == 1 and parameters[0].kind in {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }:
        return True
    raise SynapsysConfigError(
        f"[Synapsys] {description} must accept zero arguments or one StopSignal."
    )


def _run_awaitable(
    awaitable: Awaitable[Any],
    signal: StopSignal | None,
    application_loop: asyncio.AbstractEventLoop | None,
) -> Any:
    if application_loop is not None:
        if application_loop.is_closed():
            raise RuntimeError("The application event loop is closed")

        async def await_result() -> Any:
            return await awaitable

        future = asyncio.run_coroutine_threadsafe(await_result(), application_loop)
        if signal is not None:

            def cancel_future() -> None:
                future.cancel()

            signal._set_cancel_callback(cancel_future)
        try:
            return future.result()
        finally:
            if signal is not None:
                signal._clear_cancel_callback()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        task = asyncio.ensure_future(awaitable, loop=loop)
        if signal is not None:

            def cancel_task() -> None:
                loop.call_soon_threadsafe(task.cancel)

            signal._set_cancel_callback(cancel_task)
        return loop.run_until_complete(task)
    finally:
        if signal is not None:
            signal._clear_cancel_callback()
        with contextlib.suppress(BaseException):
            loop.run_until_complete(loop.shutdown_asyncgens())
        asyncio.set_event_loop(None)
        loop.close()


class _null_scope:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: Any) -> None:
        return None
