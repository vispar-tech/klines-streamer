"""Type definitions and utilities for the streamer package."""

import re
from typing import ClassVar, Dict


class Interval:
    """
    Time interval representation with support for various formats.

    Supports formats like: 1m, 10s, 50m, 1h, 1D, 2M, 500ms
    where:
    - ms = milliseconds (minimum 100ms)
    - s = seconds
    - m = minutes
    - h = hours
    - D = days
    - M = months (30 days)
    """

    # Conversion factors to milliseconds
    _UNITS: ClassVar[Dict[str, int]] = {
        "ms": 1,  # milliseconds
        "s": 1000,  # seconds to milliseconds
        "m": 60 * 1000,  # minutes to milliseconds
        "h": 60 * 60 * 1000,  # hours to milliseconds
        "D": 24 * 60 * 60 * 1000,  # days to milliseconds
        "M": 30 * 24 * 60 * 60 * 1000,  # months (30 days) to milliseconds
    }

    _PATTERN = re.compile(r"^(\d+)(ms|[smhDM])$")
    _MIN_MILLISECONDS = 100

    def __init__(self, interval_str: str) -> None:
        """
        Initialize interval from string format.

        Args:
            interval_str: Interval string (e.g., "1m", "5h", "1D", "500ms")

        Raises:
            ValueError: If interval format is invalid or milliseconds < 100
        """
        self.raw = interval_str
        self._parse(interval_str)

    def _parse(self, interval_str: str) -> None:
        """Parse interval string and extract value and unit."""
        match = self._PATTERN.match(interval_str)
        if not match:
            raise ValueError(
                f"Invalid interval format: {interval_str}. "
                f"Expected format: <number><unit> where unit is one of "
                f"{', '.join(list(self._UNITS.keys()))}"
            )

        self.value = int(match.group(1))
        self.unit = match.group(2)

        if self.value <= 0:
            raise ValueError(f"Interval value must be positive, got: {self.value}")

        # Special validation for milliseconds
        if self.unit == "ms" and self.value < self._MIN_MILLISECONDS:
            raise ValueError(
                f"Millisecond intervals must be at least {self._MIN_MILLISECONDS}ms, "
                f"got: {self.value}ms"
            )

    def to_milliseconds(self) -> int:
        """Convert interval to milliseconds."""
        return self.value * self._UNITS[self.unit]

    def to_seconds(self) -> int:
        """Convert interval to seconds."""
        return self.to_milliseconds() // 1000

    def to_minutes(self) -> int:
        """Convert interval to minutes."""
        return self.to_milliseconds() // (60 * 1000)

    def __str__(self) -> str:
        """Representation of the interval."""
        return self.raw

    def __repr__(self) -> str:
        """Developer representation."""
        return f"Interval('{self.raw}')"

    def __eq__(self, other: object) -> bool:
        """Check equality based on milliseconds."""
        if not isinstance(other, Interval):
            return NotImplemented
        return self.to_milliseconds() == other.to_milliseconds()

    def __hash__(self) -> int:
        """Hash based on milliseconds."""
        return hash(self.to_milliseconds())

    def __lt__(self, other: "Interval") -> bool:
        """Compare intervals."""
        return self.to_milliseconds() < other.to_milliseconds()
