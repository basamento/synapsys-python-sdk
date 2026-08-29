"""Process-scoped stdout/stderr capture with bounded buffering."""

from __future__ import annotations

import contextvars
import sys
import threading
import traceback
from collections import deque
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TextIO

MAX_QUEUE_ENTRIES = 10_000
MAX_LINE_CHARS = 16_000
MAX_PARTIAL_BYTES = MAX_LINE_CHARS * 4


@dataclass
class _Scope:
    capture: LogCapture
    process_name: str
    run_id: int | None
    sequence: int = 0
    partial: MutableMapping[str, str] = field(default_factory=lambda: {"stdout": "", "stderr": ""})


_scope_var: contextvars.ContextVar[_Scope | None] = contextvars.ContextVar(
    "synapsys_log_scope", default=None
)
_suspended: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "synapsys_capture_suspended", default=False
)
_install_lock = threading.RLock()
_active: LogCapture | None = None
_installed: Mapping[str, Any] | None = None


@contextmanager
def suspend_capture() -> Iterator[None]:
    token = _suspended.set(True)
    try:
        yield
    finally:
        _suspended.reset(token)


class LogCapture:
    """Tee process output to Core without redirecting the application's console."""

    def __init__(
        self,
        *,
        max_queue_entries: int = MAX_QUEUE_ENTRIES,
        max_line_chars: int = MAX_LINE_CHARS,
    ) -> None:
        self.max_queue_entries = max_queue_entries
        self.max_line_chars = max_line_chars
        self._queue: deque[dict[str, Any]] = deque()
        self._queue_lock = threading.RLock()

    @property
    def is_installed(self) -> bool:
        with _install_lock:
            return _active is self

    @property
    def size(self) -> int:
        with self._queue_lock:
            return len(self._queue)

    def install(self) -> None:
        global _active, _installed
        with _install_lock:
            if _active is self:
                return
            if _active is None:
                original_out = sys.stdout
                original_err = sys.stderr
                out_proxy = _Tee("stdout", original_out)
                err_proxy = _Tee("stderr", original_err)
                sys.stdout = out_proxy
                sys.stderr = err_proxy
                _installed = {
                    "original_out": original_out,
                    "original_err": original_err,
                    "out_proxy": out_proxy,
                    "err_proxy": err_proxy,
                }
            _active = self

    def uninstall(self) -> None:
        global _active, _installed
        with _install_lock:
            if _active is not self:
                return
            _active = None
            installed = _installed
            if installed is not None:
                if sys.stdout is installed["out_proxy"]:
                    sys.stdout = installed["original_out"]
                if sys.stderr is installed["err_proxy"]:
                    sys.stderr = installed["original_err"]
                _installed = None

    @contextmanager
    def scope(self, process_name: str, run_id: int | None) -> Iterator[None]:
        scope = _Scope(self, process_name, run_id)
        token = _scope_var.set(scope)
        try:
            yield
        finally:
            self._flush_partials(scope)
            _scope_var.reset(token)

    def capture_exception(self, error: BaseException) -> None:
        scope = _scope_var.get()
        if scope is None or scope.capture is not self:
            return
        for line in traceback.format_exception(type(error), error, error.__traceback__):
            for part in line.rstrip("\n").splitlines():
                self._enqueue(scope, "stderr", part)

    def drain(self, max_entries: int, max_bytes: int) -> list[dict[str, Any]]:
        batch: list[dict[str, Any]] = []
        byte_count = 0
        with self._queue_lock:
            while self._queue and len(batch) < max_entries:
                next_entry = self._queue[0]
                next_bytes = len(str(next_entry["message"]).encode("utf-8"))
                if batch and byte_count + next_bytes > max_bytes:
                    break
                batch.append(self._queue.popleft())
                byte_count += next_bytes
        return batch

    def requeue_front(self, entries: list[dict[str, Any]]) -> None:
        if not entries:
            return
        with self._queue_lock:
            for entry in reversed(entries):
                if len(self._queue) >= self.max_queue_entries:
                    self._queue.pop()
                self._queue.appendleft(entry)

    def entries(self) -> list[Mapping[str, Any]]:
        """Return a test/debug snapshot without exposing the mutable queue."""
        with self._queue_lock:
            return list(self._queue)

    def _absorb(self, scope: _Scope, source: str, text: str) -> None:
        incoming = scope.partial[source] + text
        lines = incoming.split("\n")
        scope.partial[source] = lines.pop()
        for line in lines:
            self._enqueue(scope, source, line)

        pending = scope.partial[source]
        if len(pending.encode("utf-8")) > MAX_PARTIAL_BYTES:
            self._enqueue(scope, source, pending)
            scope.partial[source] = ""

    def _flush_partials(self, scope: _Scope) -> None:
        for source in ("stdout", "stderr"):
            pending = scope.partial[source]
            if pending:
                self._enqueue(scope, source, pending)
            scope.partial[source] = ""

    def _enqueue(self, scope: _Scope, source: str, raw_line: str) -> None:
        if scope.run_id is None:
            return
        line = raw_line.rstrip("\r")
        if not line.strip():
            return
        message = line
        if len(message) > self.max_line_chars:
            message = message[: self.max_line_chars] + "…"

        scope.sequence += 1
        entry: dict[str, Any] = {
            "processName": scope.process_name,
            "runId": scope.run_id,
            "sequence": scope.sequence,
            "timestamp": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "source": source,
            "level": "error" if source == "stderr" else "info",
            "message": message,
        }
        with self._queue_lock:
            if len(self._queue) >= self.max_queue_entries:
                self._queue.popleft()
            self._queue.append(entry)


class _Tee:
    def __init__(self, source: str, original: TextIO) -> None:
        self._source = source
        self._original = original

    def write(self, text: str) -> int:
        result = self._original.write(text)
        try:
            capture = _active
            scope = _scope_var.get()
            if (
                capture is not None
                and scope is not None
                and scope.capture is capture
                and not _suspended.get()
            ):
                capture._absorb(scope, self._source, str(text))
        except BaseException:
            pass
        return result

    def flush(self) -> None:
        self._original.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)
