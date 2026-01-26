"""Type definitions and utilities for the streamer package."""

import re
from typing import Any, ClassVar, Dict, Literal, Union

Channel = Literal["linear", "spot"]
DataType = Literal["klines", "ticker", "price", "tickers-klines"]


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
    _UNITS: ClassVar[Dict[str, int]] = {
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

    def __init__(self, interval: Union[str, int]) -> None:
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

    def to_bybit(self) -> str:
        """Convert to Bybit interval string, or raise ValueError if unsupported."""
        ms = self.to_milliseconds()
        # Minutes intervals supported by Bybit
        minutes_set = {1, 3, 5, 15, 30, 60, 120, 240, 360, 720}
        minute_ms_map = {val: val * 60_000 for val in minutes_set}

        for minute, minute_ms in minute_ms_map.items():
            if ms == minute_ms:
                return str(minute)
        # Day
        if ms == 86_400_000:
            return "D"
        # Week
        if ms == 604_800_000:
            return "W"
        # Month, treated as exactly 30 days
        if ms == 2_592_000_000:
            return "M"
        raise ValueError(f"Interval '{self!s}' cannot be represented in Bybit format")

    def to_bingx(self) -> str:
        """Convert to BingX interval string, or raise ValueError if unsupported."""
        ms = self.to_milliseconds()
        # Minutes intervals supported by BingX (similar to Bybit)
        minutes_set = {1, 3, 5, 15, 30, 60, 120, 240, 360, 720}
        minute_ms_map = {val: val * 60_000 for val in minutes_set}

        for minute, minute_ms in minute_ms_map.items():
            if ms == minute_ms:
                return str(minute)
        # Day
        if ms == 86_400_000:
            return "D"
        # Week
        if ms == 604_800_000:
            return "W"
        # Month, treated as exactly 30 days
        if ms == 2_592_000_000:
            return "M"
        raise ValueError(f"Interval '{self!s}' cannot be represented in BingX format")

    def to_websocket_bitget(self, utc: bool = False) -> str:
        """
        Convert to Bitget channel name string, or raise ValueError if unsupported.

        Formats:
            candle1m (1 minute), candle5m (5 minutes), candle15m (15 minutes),
            candle30m (30 minutes), candle1H (1 hour), candle4H (4 hours),
            candle6H (6 hours), candle6Hutc (6 hours utc), candle12H (12 hours),
            candle12Hutc (12 hours utc), candle3D (3 days), candle3Dutc,
            candle1D (1 day), candle1Dutc, candle1W (1 week), candle1Wutc,
            candle1M (1 month), candle1Mutc
        utc: If True, use UTC variant (where available).
        """
        ms = self.to_milliseconds()
        bitget_map = {
            60_000: "candle1m",
            300_000: "candle5m",
            900_000: "candle15m",
            1_800_000: "candle30m",
            3_600_000: "candle1H",
            14_400_000: "candle4H",
            21_600_000: "candle6H",
            43_200_000: "candle12H",
            86_400_000: "candle1D",
            259_200_000: "candle3D",
            604_800_000: "candle1W",
            2_592_000_000: "candle1M",
        }
        bitget_map_utc = {
            21_600_000: "candle6Hutc",
            43_200_000: "candle12Hutc",
            86_400_000: "candle1Dutc",
            259_200_000: "candle3Dutc",
            604_800_000: "candle1Wutc",
            2_592_000_000: "candle1Mutc",
        }
        # UTC variants only for particular intervals
        if utc and ms in bitget_map_utc:
            return bitget_map_utc[ms]
        # Regular intervals
        if ms in bitget_map:
            return bitget_map[ms]
        raise ValueError(
            f"Interval '{self!s}' cannot be represented as a Bitget channel name"
        )

    def to_bitget(self, utc: bool = False) -> str:
        """
        Convert to Bitget K-Line granularity string, or raise ValueError if unsupported.

        Bitget K-line particle size:
            - 1m (1 minute)
            - 3m (3 minutes)
            - 5m (5 minutes)
            - 15m (15 minutes)
            - 30m (30 minutes)
            - 1H (1 hour)
            - 4H (4 hours)
            - 6H (6 hours)
            - 12H (12 hours)
            - 1D (1 day)
            - 3D (3 days)
            - 1W (1 week)
            - 1M (1 month)
            - 6Hutc (UTC 6 hour line)
            - 12Hutc (UTC 12 hour line)
            - 1Dutc (UTC 1-day line)
            - 3Dutc (UTC 3-day line)
            - 1Wutc (UTC weekly line)
            - 1Mutc (UTC monthly line)

        Args:
            utc (bool): If True, use the UTC variant string where available.

        Returns:
            str: Bitget K-line granularity string.

        Raises:
            ValueError: If the interval cannot be represented.
        """
        ms = self.to_milliseconds()
        mapping = {
            60_000: "1m",
            180_000: "3m",
            300_000: "5m",
            900_000: "15m",
            1_800_000: "30m",
            3_600_000: "1H",
            14_400_000: "4H",
            21_600_000: "6H",
            43_200_000: "12H",
            86_400_000: "1D",
            259_200_000: "3D",
            604_800_000: "1W",
            2_592_000_000: "1M",
        }
        mapping_utc = {
            21_600_000: "6Hutc",
            43_200_000: "12Hutc",
            86_400_000: "1Dutc",
            259_200_000: "3Dutc",
            604_800_000: "1Wutc",
            2_592_000_000: "1Mutc",
        }
        if utc and ms in mapping_utc:
            return mapping_utc[ms]
        if ms in mapping:
            return mapping[ms]
        raise ValueError(
            f"Interval '{self!s}' cannot be represented in Bitget granularity format"
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
