"""Bybit WebSocket client implementation."""

import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, Set

import orjson
from websockets import Data
from websockets.asyncio.client import ClientConnection

from streamer.settings import settings
from streamer.types import Channel
from streamer.websockets.base import WebSocketClient

logger = logging.getLogger(__name__)


class BybitWebSocketClient(WebSocketClient):
    """Bybit WebSocket client with socket pool support."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
    ) -> None:
        """Initialize Bybit WebSocket client."""
        super().__init__(channel, on_trade, on_ticker)

        self.topic: str | None = None
        self.ticker_topic: str | None = None

        self.topic_part = "publicTrade."
        if settings.enable_klines_stream or settings.enable_trades_stream:
            self.topic = "publicTrade.{symbol}"

        self.ticker_topic_part = "tickers."
        if (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        ):
            self.ticker_topic = "tickers.{symbol}"

    def _get_websocket_url(self, channel: Channel) -> str:
        """Return the Bybit WebSocket URL."""
        return f"wss://stream.bybit.com/v5/public/{channel}"

    async def _subscribe(self, websocket: ClientConnection, symbols: Set[str]) -> None:
        """Subscribe to streams for assigned symbols (including tickers)."""
        args: list[str] = []
        # Add main stream topics (trades)
        if self.topic:
            args += [self.topic.format(symbol=symbol) for symbol in symbols]
        # Always add tickers.{symbol} for each symbol
        if self.ticker_topic:
            args += [self.ticker_topic.format(symbol=symbol) for symbol in symbols]

        subscription_msg = {"op": "subscribe", "args": args}

        await websocket.send(orjson.dumps(subscription_msg).decode("utf-8"))
        logger.info(f"Subscribed to streams: {args} on channel {self.channel}")

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""
        while self._running:
            try:
                ping_msg = {"req_id": f"ping_{socket_id}", "op": "ping"}
                await websocket.send(orjson.dumps(ping_msg).decode("utf-8"))
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(
                    f"Ping error on channel {self.channel} (socket {socket_id}): {e}"
                )
                break

    async def _handle_message(self, message: Data, websocket: ClientConnection) -> None:
        """Handle incoming WebSocket message."""
        try:
            data = orjson.loads(message)

            # Skip ping responses and subscription confirmations
            if data.get("op") == "ping" and data.get("ret_msg") == "pong":
                return
            if data.get("op") == "subscribe":
                logger.debug(
                    f"Subscription confirmed on channel {self.channel}: {data}"
                )
                return

            topic = data.get("topic", "")
            # Handle both main data stream (trade/kline) and tickers
            if not (topic.startswith((self.topic_part, self.ticker_topic_part))):
                return

            trades_or_data = data.get("data")
            if not trades_or_data:
                return

            if topic.startswith(self.ticker_topic_part):
                await self.on_ticker(data)
                return

            # Handle trade data
            await self.on_trade(data)

        except orjson.JSONDecodeError as e:
            logger.error(
                f"Failed to parse message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
