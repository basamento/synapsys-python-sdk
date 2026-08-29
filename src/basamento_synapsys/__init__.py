"""Synapsys Python SDK public API."""

from ._process import ProcessState, ProcessType
from ._stop import StopSignal
from .client import RegisteredProcess, Synapsys
from .errors import (
    StopRequested,
    SynapsysConfigError,
    SynapsysError,
    SynapsysStartupError,
)

__all__ = [
    "ProcessState",
    "ProcessType",
    "RegisteredProcess",
    "StopRequested",
    "StopSignal",
    "Synapsys",
    "SynapsysConfigError",
    "SynapsysError",
    "SynapsysStartupError",
]

__version__ = "0.1.0"
