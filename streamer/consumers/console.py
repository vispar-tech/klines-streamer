"""Print kline data to the console."""

from typing import Any, Dict, List

from streamer.consumers.base import BaseConsumer
from streamer.storage import Storage
from streamer.types import Channel, DataType


class ConsoleConsumer(BaseConsumer):
    """Print kline data to the console."""

    def __init__(self, storage: Storage, name: str = "console") -> None:
        """Initialize ConsoleConsumer."""
        super().__init__(storage, name)

    def validate(self) -> None:
        """Validate console consumer settings."""

    async def setup(self) -> None:
        """Set up the console consumer."""
        self.logger.info("Setting up console consumer")

    async def start(self) -> None:
        """Start the console consumer."""
        self.logger.info("Starting console consumer")
        self._is_running = True

    async def consume(
        self,
        channel: Channel,
        data_type: DataType,
        data: List[Dict[str, Any]],
    ) -> None:
        """Print streaming data to console."""
        if not self._is_running or not data:
            return

        for item in data:
            symbol = item.get("symbol", "unknown")
            if data_type == "klines":
                interval = item.get("interval", "unknown")
                ts = item.get("timestamp", "unknown")
                open_ = item.get("open", "")
                high = item.get("high", "")
                low = item.get("low", "")
                close = item.get("close", "")
                # Print summary of kline (candlestick) OHLC values
                self.logger.info(
                    f"[{channel.upper()}] [{symbol}] [{interval}] {ts} "
                    f"open={open_} high={high} low={low} close={close}"
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
        """Stop the console consumer."""
        self.logger.info("Stopping console consumer")
        self._is_running = False
