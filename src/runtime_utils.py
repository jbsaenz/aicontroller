"""Runtime utility helpers shared across API and worker modules."""

from __future__ import annotations

from typing import Any

TRUTHY_VALUES = {"1", "true", "yes", "on"}
VALID_CONTROL_MODES = {"advisory", "actuation"}
DEFAULT_CONTROL_MODE = "advisory"


def is_truthy(value: Any, default: bool = False) -> bool:
    """Interpret common truthy string/int values with an optional None default."""
    if value is None:
        return default
    return str(value).strip().lower() in TRUTHY_VALUES


def normalize_control_mode(
    value: Any, default: str = DEFAULT_CONTROL_MODE
) -> str:
    """Normalize control mode to one of advisory/actuation."""
    mode = str(value if value is not None else default).strip().lower()
    if mode not in VALID_CONTROL_MODES:
        return DEFAULT_CONTROL_MODE
    return mode
