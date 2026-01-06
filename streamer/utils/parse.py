"""Utility functions for parsing number fields in Bybit data dictionaries.

This module provides helpers for type coercion of numeric fields
(commonly encoded as strings) into Python native floats or ints.
"""

import contextlib
from typing import Any


def parse_numbers(d: dict[str, Any]) -> dict[str, Any]:
    """Convert string numbers in a dict to float or int where possible."""
    out = d.copy()
    for k, v in out.items():
        if k == "nextFundingTime":
            with contextlib.suppress(ValueError, TypeError):
                out[k] = int(v)
        elif isinstance(v, str):
            with contextlib.suppress(ValueError, TypeError):
                out[k] = float(v)
    return out
