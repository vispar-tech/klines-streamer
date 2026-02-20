"""BingX WebSocket client implementation."""

import asyncio
import gzip
import io
import logging
import uuid
from typing import Any, Callable, Coroutine, Dict, Set

import orjson
from websockets import Data
from websockets.asyncio.client import ClientConnection

from streamer.settings import settings
from streamer.types import Channel
from streamer.websockets.base import WebSocketClient

logger = logging.getLogger(__name__)


class BingxWebSocketClient(WebSocketClient):
    """BingX WebSocket client with socket pool support."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Initialize BingX WebSocket client."""
        super().__init__(channel, on_trade, on_ticker, on_symbols_count_changed)

        # BingX uses @trade and @ticker suffixes, not prefixes
        self.trade_enabled = settings.enable_klines_stream
        self.ticker_enabled = (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        )

        # BingX limits validation
        self._validate_bingx_limits()

    def _validate_bingx_limits(self) -> None:
        """Validate BingX WebSocket limits."""
        # Calculate how symbols are distributed across sockets
        if self.pool_size <= 1:
            symbols_per_socket = len(self.symbols)
        else:
            # Simple round-robin distribution
            symbols_per_socket = (
                len(self.symbols) + self.pool_size - 1
            ) // self.pool_size

        # Calculate topics per socket (trade + ticker per symbol)
        topics_per_socket = symbols_per_socket
        if self.trade_enabled and self.ticker_enabled:
            topics_per_socket *= 2  # Both trade and ticker topics
        elif self.trade_enabled or self.ticker_enabled:
            topics_per_socket *= 1  # Only one type of topics

        # BingX limit: maximum 200 topics per websocket
        if topics_per_socket > 200:
            raise ValueError(
                f"BingX limit exceeded: {topics_per_socket} topics per socket "
                f"(maximum 200 allowed). Reduce number of symbols, "
                f"increase pool_size, or disable streams."
            )

        # BingX limit: maximum 60 websockets per IP
        if self.pool_size > 60:
            raise ValueError(
                f"BingX limit exceeded: {self.pool_size} websockets "
                "(maximum 60 per IP allowed)"
            )

        logger.info(
            f"BingX limits validated: ~{topics_per_socket} topics per socket "
            f"({symbols_per_socket} symbols/socket), {self.pool_size} websockets"
        )

    def _get_websocket_url(self, channel: Channel) -> str:
        """Return the BingX WebSocket URL."""
        # BingX uses a single market data endpoint
        return "wss://open-api-swap.bingx.com/swap-market"

    async def _subscribe(self, websocket: ClientConnection, symbols: Set[str]) -> None:
        """Subscribe to streams for assigned symbols (including tickers)."""
        # Subscribe to each symbol's streams
        for symbol in symbols:
            subscription_id = str(uuid.uuid4())

            # Subscribe to trades if enabled
            if self.trade_enabled:
                trade_subscription = {
                    "id": subscription_id,
                    "reqType": "sub",
                    "dataType": f"{symbol}@trade",
                }
                await websocket.send(orjson.dumps(trade_subscription).decode("utf-8"))

            # Subscribe to tickers if enabled
            if self.ticker_enabled:
                ticker_subscription = {
                    "id": subscription_id,
                    "reqType": "sub",
                    "dataType": f"{symbol}@ticker",
                }
                await websocket.send(orjson.dumps(ticker_subscription).decode("utf-8"))

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""
        # BingX doesn't require periodic pings from client
        # Ping/pong is handled in message processing
        await asyncio.sleep(0)  # Keep the method async

    async def _handle_message(self, message: Data, websocket: ClientConnection) -> None:
        """Handle incoming WebSocket message."""
        try:
            if isinstance(message, str):
                return

            # Handle binary (compressed) messages
            try:
                # Decompress gzip data
                compressed_data = gzip.GzipFile(fileobj=io.BytesIO(message), mode="rb")
                decompressed_data = compressed_data.read()
                message = decompressed_data.decode("utf-8")

                if message.lower() == "ping":
                    # Respond with "Pong"
                    await websocket.send("Pong")
                    return

                # Parse JSON
                data = orjson.loads(message)
            except (
                gzip.BadGzipFile,
                UnicodeDecodeError,
                orjson.JSONDecodeError,
            ) as e:
                logger.error(
                    f"Failed to decompress/parse message: {e}, message: {message!s}"
                )
                return

            # Simplified subscription confirmation handling
            if (
                "code" in data
                and data["code"] == 0
                and "data" in data
                and data["data"] is None
            ):
                return

            # Handle market data
            if "data" in data and "dataType" in data:
                data_type = data["dataType"]

                if "@ticker" in data_type:
                    await self.on_ticker(data)
                elif "@trade" in data_type:
                    await self.on_trade(data)

                else:
                    logger.warning(
                        f"Unknown data type: {data_type}, message: {message!s}"
                    )

        except Exception as e:
            logger.exception(
                f"Failed to handle message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
