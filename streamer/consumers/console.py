"""Console consumer for printing kline data to the console."""

from typing import Any, Dict

from streamer.consumers.base import BaseConsumer


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

    async def consume(self, data: Dict[str, Any]) -> None:
        """
        Print kline data to console.

        Args:
            data: Kline data dictionary containing symbol, interval, and candle data.
        """
        if not self._is_running:
            return

        symbol = data.get("symbol", "unknown")
        interval = data.get("interval", "unknown")
        info_str = " ".join(
            f"{k}={v!r}" for k, v in data.items() if k not in {"symbol", "interval"}
        )
        self.logger.info(f"[{symbol}] [{interval}] {info_str}")

    async def stop(self) -> None:
        """
        Stop the console consumer.

        Marks the consumer as stopped and performs any necessary cleanup.
        """
        self.logger.info("Stopping console consumer")
        self._is_running = False
