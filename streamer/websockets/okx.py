"""Okx WebSocket client implementation."""

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


class OkxWebSocketClient(WebSocketClient):
    """Okx WebSocket client with socket pool support."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Initialize Okx WebSocket client."""
        super().__init__(channel, on_trade, on_ticker, on_symbols_count_changed)

        # index-tickers only, no trade nor klines supported!
        self._index_tickers_enabled = (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        )

        if settings.enable_klines_stream:
            raise RuntimeError("Currently OKX does not support klines from trades")
        if not self._index_tickers_enabled:
            raise RuntimeError(
                "No supported stream enabled for OKX (need ticker/price/tickers_kline)"
            )

    def _get_websocket_url(self, channel: Channel) -> str:
        """Return the Okx WebSocket URL. Index-tickers uses a fixed public path."""
        # Path is always /ws/v5/public for index-tickers
        return "wss://ws.okx.com:8443/ws/v5/public"

    async def _subscribe(self, websocket: ClientConnection, symbols: set[str]) -> None:
        """Subscribe to index-tickers stream for assigned symbols."""
        args: list[dict[str, str]] = []
        for symbol in symbols:
            args.append({"channel": "index-tickers", "instId": symbol})

        subscription_msg = {
            "op": "subscribe",
            "args": args,
        }

        await websocket.send(orjson.dumps(subscription_msg).decode("utf-8"))
        logger.info(
            f"Subscribed to OKX index-tickers for: "
            f"{list(symbols)} on channel {self.channel}"
        )

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""
        # OKX officially recommends ping interval < 30s
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
        """Handle incoming WebSocket message from OKX index-tickers."""
        try:
            data = orjson.loads(message)

            # Handle ping/pong (if any)
            if data.get("event") == "pong" or data.get("op") == "pong":
                return

            # Handle subscription confirmation
            if data.get("event") == "subscribe":
                logger.debug(
                    f"Subscription confirmed on channel {self.channel}: {data}"
                )
                return

            if data.get("event") == "error":
                logger.error(
                    f"Error from OKX WebSocket for channel "
                    f"{self.channel}: {data.get('msg')}"
                )
                return

            # Handle pushed index-ticker data
            arg = data.get("arg", {})
            channel = arg.get("channel", "")
            if channel != "index-tickers":
                return

            tickers_data = data["data"]
            if not tickers_data:
                return

            # Pass full message (with arg, data) to callback for consistency
            for ticker in tickers_data:
                await self.on_ticker(ticker)

        except orjson.JSONDecodeError as e:
            logger.error(
                f"Failed to parse message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
