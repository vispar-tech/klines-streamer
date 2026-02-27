"""Type definitions and utilities for the streamer package."""

import re
from typing import Any, ClassVar, Literal

Channel = Literal["linear", "spot"]
DataType = Literal["klines", "ticker", "price", "tickers-klines"]
ExchangeType = Literal["bybit", "bingx", "bitget", "binance", "okx"]


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

    Can also be constructed from integer milliseconds directly.
    """

    # Conversion factors to milliseconds
    _UNITS: ClassVar[dict[str, int]] = {
        "ms": 1,  # milliseconds
        "s": 1000,  # seconds to milliseconds
        "m": 60 * 1000,  # minutes to milliseconds
        "h": 60 * 60 * 1000,  # hours to milliseconds
        "D": 24 * 60 * 60 * 1000,  # days to milliseconds
        "W": 7 * 24 * 60 * 60 * 1000,  # weeks to milliseconds
        "M": 30 * 24 * 60 * 60 * 1000,  # months (30 days) to milliseconds
    }

    _PATTERN = re.compile(r"^(\d+)(ms|[smhDWM])$")
    _MIN_MILLISECONDS = 100

    def __init__(self, interval: str | int) -> None:
        """
        Initialize interval from string format or direct milliseconds.

        Args:
            interval: Interval string (e.g., "1m", "5h", "1D", "500ms")
                or integer milliseconds

        Raises:
            ValueError: If interval format is invalid or milliseconds < 100
        """
        if isinstance(interval, int):
            if interval < self._MIN_MILLISECONDS:
                raise ValueError(
                    f"Millisecond intervals must be at least "
                    f"{self._MIN_MILLISECONDS}ms, got: {interval}ms"
                )
            self.raw = f"{interval}ms"
            self.value = interval
            self.unit = "ms"
        else:
            self.raw = interval
            self._parse_instance(interval)

    def _parse_instance(self, interval_str: str) -> None:
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

    @classmethod
    def from_ms(cls, ms: int) -> "Interval":
        """Alternate explicit constructor for an Interval from milliseconds."""
        return cls(ms)

    @classmethod
    def parse(cls, value: Any) -> "Interval":
        """
        Parse and return an Interval instance from string, int, or Interval itself.

        Handles use in Pydantic and data validator contexts.
        """
        if isinstance(value, Interval):
            return value
        if isinstance(value, int):
            return cls(value)
        if isinstance(value, str):
            return cls(value)
        raise TypeError(
            f"Cannot convert value of type {type(value).__name__} to Interval"
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
        """
        Human-friendly representation of the interval.

        - < 1 sec: ms
        - >=1s and <1m: s
        - >=1m and <1h: m
        - >=1h and <1d: h
        - >=1d and <1w: d
        - >=1w and <1M: w
        - >=1M: M.
        """
        ms = self.to_milliseconds()
        if ms < 1000:
            suffix, value = "ms", ms
        elif ms < 60_000:
            suffix, value = "s", ms // 1000
        elif ms < 3_600_000:
            suffix, value = "m", ms // 60_000
        elif ms < 86_400_000:
            suffix, value = "h", ms // 3_600_000
        elif ms < 604_800_000:
            suffix, value = "d", ms // 86_400_000
        elif ms < 2_592_000_000:
            suffix, value = "w", ms // 604_800_000
        else:
            suffix, value = "M", ms // 2_592_000_000
        return f"{value}{suffix}"

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

    def __repr__(self) -> str:
        """Unambiguous developer representation."""
        return (
            f"Interval(raw={self.raw!r}, value={self.value}, "
            f"unit={self.unit!r}, str={self!s})"
        )
