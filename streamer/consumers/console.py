"""Console consumer for printing kline data to the console."""

from datetime import datetime
from typing import Any, Dict, List

from streamer.consumers.base import BaseConsumer
from streamer.types import Interval


class ConsoleConsumer(BaseConsumer):
    """Prints kline data to the console."""

    def __init__(self, name: str = "console") -> None:
        """
        Initialize the ConsoleConsumer.

        Args:
            name: Consumer name/identifier (defaults to "console").
        """
        super().__init__(name)

    def validate(self) -> None:
        """
        Validate console consumer settings.

        Console consumer doesn't require any specific settings.
        """

    async def setup(self) -> None:
        """
        Set up the console consumer.

        Called once before starting consumption.
        Use this method for resource allocation or preparation.
        """
        self.logger.info("Setting up console consumer")

    async def start(self) -> None:
        """
        Start the console consumer.

        Marks the consumer as running and ready to process data.
        """
        self.logger.info("Starting console consumer")
        self._is_running = True

    async def consume(self, data: List[Dict[str, Any]]) -> None:
        """
        Print kline data to console, converting timestamp to readable time.

        Args:
            data: Kline data dictionary containing symbol, interval, and kline data.
        """
        if not self._is_running:
            return

        if not data:
            return
        log_lines: list[str] = []
        for item in data:
            symbol = item.get("symbol", "unknown")
            interval_ms = item.get("interval", "unknown")
            interval = Interval(interval_ms)
            # Extract timestamp in ms and convert to readable string
            timestamp_ms = item.get("timestamp")
            if timestamp_ms is not None:
                # Convert ms to seconds and then format time
                ts_readable = datetime.fromtimestamp(timestamp_ms / 1000).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                info_items: list[str] = []
                for k, v in item.items():
                    if k == "timestamp":
                        info_items.append(f"timestamp={timestamp_ms} ({ts_readable})")
                    elif k not in {"symbol", "interval"}:
                        info_items.append(f"{k}={v!r}")
                info_str = " ".join(info_items)
            else:
                info_str = " ".join(
                    f"{k}={v!r}"
                    for k, v in item.items()
                    if k not in {"symbol", "interval"}
                )

            log_lines.append(f"[{symbol}] [{interval}] {info_str}")

        self.logger.info("\n".join(log_lines))

    async def stop(self) -> None:
        """
        Stop the console consumer.

        Marks the consumer as stopped and performs any necessary cleanup.
        """
        self.logger.info("Stopping console consumer")
        self._is_running = False
