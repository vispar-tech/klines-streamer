"""Console consumer for printing kline data to the console."""

from typing import Any, Dict, List

from streamer.consumers.base import BaseConsumer
from streamer.types import Channel, DataType


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

    async def consume(
        self,
        channel: Channel,
        data_type: DataType,
        data: List[Dict[str, Any]],
    ) -> None:
        """
        Consume and print streaming data to the console.

        Args:
            channel: The data stream channel ("linear", "spot", etc.).
            data_type: The type of data being consumed
                ("klines", "ticker", "price", "trades").
            data: A list of dictionaries with payload data.
        """
        if not self._is_running or not data:
            return

        for item in data:
            symbol = item.get("symbol", "unknown")
            if data_type == "klines":
                interval = item.get("interval", "unknown")
                ts = item.get("timestamp", "unknown")
                close = item.get("close", "")
                # Print summary of kline (candlestick) close value
                self.logger.info(
                    f"[{channel.upper()}] [{symbol}] [{interval}] {ts} close={close}"
                )
            elif data_type == "ticker":
                # Print full ticker snapshot
                self.logger.info(f"[{channel.upper()}] [TICKER] [{symbol}] {item}")
            elif data_type == "price":
                price = item.get("price", "unknown")
                # Print price update
                self.logger.info(
                    f"[{channel.upper()}] [PRICE] [{symbol}] price={price}"
                )
            elif data_type in {"trades", "trade"}:
                symbol = item.get("s", "unknown")
                trade_repr = " ".join(f"{k}={v}" for k, v in item.items())
                self.logger.info(f"[{channel.upper()}] [TRADE] [{symbol}] {trade_repr}")

    async def stop(self) -> None:
        """
        Stop the console consumer.

        Marks the consumer as stopped and performs any necessary cleanup.
        """
        self.logger.info("Stopping console consumer")
        self._is_running = False
