"""Duration parsing shared by configuration and stop signals."""

from __future__ import annotations

import re
from typing import Union

from .errors import SynapsysConfigError

Duration = Union[str, int, float]

_DURATION = re.compile(r"^([0-9]+(?:\.[0-9]+)?)(ms|s|m|h)$", re.IGNORECASE)
_MULTIPLIER = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(value: Duration, name: str) -> float:
    """Return seconds, accepting unit strings or numeric milliseconds."""
    if isinstance(value, bool):
        raise _invalid(value, name)
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000.0
    elif isinstance(value, str):
        text = value.strip()
        match = _DURATION.fullmatch(text)
        if match is None:
            if text.replace(".", "", 1).isdigit():
                raise SynapsysConfigError(
                    f"[Synapsys] {name} must include a unit, such as '30s' or '250ms'; "
                    "a plain number means milliseconds."
                )
            raise _invalid(value, name)
        seconds = float(match.group(1)) * _MULTIPLIER[match.group(2).lower()]
    else:
        raise _invalid(value, name)

    if seconds <= 0:
        raise SynapsysConfigError(f"[Synapsys] {name} must be greater than zero.")
    return seconds


def parse_whole_seconds(value: Duration, name: str) -> int:
    seconds = parse_duration(value, name)
    if not seconds.is_integer():
        raise SynapsysConfigError(
            f"[Synapsys] {name} must resolve to a positive whole number of seconds."
        )
    return int(seconds)


def format_duration(seconds: float) -> str:
    if seconds.is_integer():
        return f"{int(seconds)}s"
    return f"{seconds:g}s"


def _invalid(value: object, name: str) -> SynapsysConfigError:
    return SynapsysConfigError(
        f"[Synapsys] Invalid {name}: {value!r}. Use a duration such as '30s', "
        "'5m', '250ms' or a numeric millisecond value."
    )
