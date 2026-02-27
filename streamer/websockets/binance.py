"""Binance WebSocket client implementation."""

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

import orjson
from websockets import Data
from websockets.asyncio.client import ClientConnection

from streamer.settings import settings
from streamer.types import Channel
from streamer.websockets.base import WebSocketClient

logger = logging.getLogger(__name__)


class BinanceWebSocketClient(WebSocketClient):
    """Binance WebSocket client with socket pool support."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Initialize Binance WebSocket client."""
        super().__init__(channel, on_trade, on_ticker, on_symbols_count_changed)

        # index-tickers only, no trade nor klines supported!
        self._index_tickers_enabled = (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        )

        if settings.enable_klines_stream:
            raise RuntimeError("Currently BINANCE does not support klines from trades")
        if not self._index_tickers_enabled:
            raise RuntimeError(
                "No supported stream enabled for BINANCE "
                "(need ticker/price/tickers_kline)"
            )

    def _get_websocket_url(self, channel: Channel) -> str:
        """Return the Binance WebSocket URL. Index-tickers uses a fixed public path."""
        # Path is always /ws/v5/public for index-tickers
        return "wss://fstream.binance.com/ws"

    async def _subscribe(self, websocket: ClientConnection, symbols: set[str]) -> None:
        """Subscribe to ticker streams for assigned symbols in BINANCE format."""
        # Binance expects a list of stream params like "<symbol>@ticker"
        params = [f"{symbol.lower()}@ticker" for symbol in symbols]

        subscription_msg = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1,
        }

        await websocket.send(orjson.dumps(subscription_msg).decode("utf-8"))
        logger.info(
            f"Subscribed to BINANCE tickers for: {params} on channel {self.channel}"
        )

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""
        # BINANCE officially recommends ping interval < 30s
        while self._running:
            try:
                await websocket.ping()
                await asyncio.sleep(25)
            except Exception as e:
                logger.error(
                    f"Ping error on channel {self.channel} (socket {socket_id}): {e}"
                )
                break

    async def _handle_message(self, message: Data, websocket: ClientConnection) -> None:
        """Handle incoming WebSocket message from BINANCE index-tickers."""
        try:
            data = orjson.loads(message)

            # Handle ping/pong (if any)
            if data.get("e") == "pong" or data.get("op") == "pong":
                return

            if data.get("e") == "error":
                logger.error(
                    f"Error from BINANCE WebSocket for channel "
                    f"{self.channel}: {data.get('msg')}"
                )
                return

            # Handle pushed index-ticker data
            channel = data.get("e", "")
            if channel != "24hrTicker":
                return

            if not data:
                return

            # Pass full message (with arg, data) to callback for consistency
            await self.on_ticker(data)

        except orjson.JSONDecodeError as e:
            logger.error(
                f"Failed to parse message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
