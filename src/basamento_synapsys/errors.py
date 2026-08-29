"""Public exception types raised by the Synapsys SDK."""


class SynapsysError(Exception):
    """Base class for SDK errors."""


class SynapsysConfigError(SynapsysError, ValueError):
    """Raised when Synapsys configuration or registration is invalid."""


class SynapsysStartupError(SynapsysError):
    """Raised when fail-fast startup cannot reach Synapsys Core."""


class StopRequested(SynapsysError):
    """Cooperative cancellation raised by :class:`StopSignal`."""

    def __init__(self, process_name: str) -> None:
        super().__init__(f"[Synapsys] Stop requested for process '{process_name}'")
        self.process_name = process_name
