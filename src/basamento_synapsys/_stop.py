"""Cooperative cancellation handed to process functions."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import threading
from typing import Callable

from .errors import StopRequested


class StopSignal:
    """A thread-safe cooperative stop signal for sync and async functions."""

    def __init__(self, process_name: str) -> None:
        self._process_name = process_name
        self._event = threading.Event()
        self._lock = threading.RLock()
        self._cancel: Callable[[], None] | None = None
        self._reason = StopRequested(process_name)

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    @property
    def event(self) -> threading.Event:
        """The underlying event, for integration with compatible blocking APIs."""
        return self._event

    def is_stop_requested(self) -> bool:
        return self.requested

    def raise_if_requested(self) -> None:
        if self.requested:
            raise self._reason

    def throw_if_cancelled(self) -> None:
        self.raise_if_requested()

    def throw_if_stopped(self) -> None:
        self.raise_if_requested()

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until stopped; return ``True`` when a stop caused the wake-up."""
        return self._event.wait(timeout)

    async def wait_async(self, timeout: float | None = None) -> bool:
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while not self.requested:
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False
                await asyncio.sleep(min(remaining, 0.05))
            else:
                await asyncio.sleep(0.05)
        return True

    def _request(self) -> None:
        self._event.set()
        with self._lock:
            cancel = self._cancel
        if cancel is not None:
            with contextlib.suppress(BaseException):
                cancel()

    def _set_cancel_callback(self, callback: Callable[[], None]) -> None:
        with self._lock:
            self._cancel = callback
            already_requested = self.requested
        if already_requested:
            callback()

    def _clear_cancel_callback(self) -> None:
        with self._lock:
            self._cancel = None

    def _is_cancellation(self, error: BaseException) -> bool:
        if error is self._reason:
            return True
        return self.requested and isinstance(
            error,
            (StopRequested, asyncio.CancelledError, concurrent.futures.CancelledError),
        )
