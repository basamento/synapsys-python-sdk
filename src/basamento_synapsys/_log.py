"""Small adapter over Python's ambient logging framework."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

_PREFIX = "[Synapsys] "
_LEVELS = {"debug": 10, "info": 20, "warning": 30, "error": 40, "silent": 100}


class SynapsysLogger:
    def __init__(self, logger: Any = None, level: str = "info") -> None:
        self._logger = logger if logger is not None else logging.getLogger("basamento_synapsys")
        self._minimum = _LEVELS[level]

    def debug(self, message: str, cause: BaseException | None = None) -> None:
        self._write("debug", message, cause)

    def info(self, message: str, cause: BaseException | None = None) -> None:
        self._write("info", message, cause)

    def warning(self, message: str, cause: BaseException | None = None) -> None:
        self._write("warning", message, cause)

    def error(self, message: str, cause: BaseException | None = None) -> None:
        self._write("error", message, cause)

    def _write(self, level: str, message: str, cause: BaseException | None) -> None:
        if _LEVELS[level] < self._minimum:
            return
        method = getattr(self._logger, level, None)
        if method is None and level == "warning":
            method = getattr(self._logger, "warn", None)
        if method is None:
            return
        suffix = ""
        if cause is not None:
            suffix = f" | {type(cause).__name__}: {cause}"
        try:
            with _without_capture():
                method(_PREFIX + message + suffix)
        except BaseException:
            # A user's logger is outside the SDK's trust boundary.
            return


@contextmanager
def _without_capture() -> Iterator[None]:
    # Imported lazily to avoid a module cycle during package import.
    try:
        from ._capture import suspend_capture
    except ImportError:
        yield
        return
    with suspend_capture():
        yield
