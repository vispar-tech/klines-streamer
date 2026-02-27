"""Bitget WebSocket client implementation."""

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


class BitgetWebSocketClient(WebSocketClient):
    """Bitget WebSocket client with socket pool support."""

    # Exchange-imposed limits
    BITGET_MAX_CONNECTIONS_PER_IP = 100
    BITGET_MAX_CHANNELS_PER_CONN = 1000  # Official, but recommend <50 for stability
    BITGET_RECOMMENDED_MAX_CHANNELS = 50  # For stability, per official docs

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Initialize Bitget WebSocket client."""
        super().__init__(channel, on_trade, on_ticker, on_symbols_count_changed)

        # Bitget USDT Perpetual topics
        self.inst_type = "USDT-FUTURES"
        self.trade_enabled = settings.enable_klines_stream
        self.ticker_enabled = (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        )

        # Connection/Subscription limits enforcement
        self._validate_bitget_limits()

        # Ping/pong watchdog
        self._pong_received_event = asyncio.Event()
        self._last_pong: float | None = None

    def _validate_bitget_limits(self) -> None:
        """Validate Bitget limits for connection/channel count."""
        # Determine how many symbols per socket
        if self.pool_size <= 1:
            symbols_per_socket = len(self.symbols)
        else:
            symbols_per_socket = (
                len(self.symbols) + self.pool_size - 1
            ) // self.pool_size

        # Each symbol may be: trade, ticker, or both
        # Each subscription to 'trade' or 'ticker' is a channel
        topics_per_socket = 0
        if self.trade_enabled:
            topics_per_socket += symbols_per_socket
        if self.ticker_enabled:
            topics_per_socket += symbols_per_socket

        # Enforce Bitget limits:
        if topics_per_socket > self.BITGET_MAX_CHANNELS_PER_CONN:
            raise ValueError(
                f"Bitget: Too many channels per connection ({topics_per_socket}, "
                f"max allowed {self.BITGET_MAX_CHANNELS_PER_CONN})"
                "reduce number of symbols, increase pool_size, "
                "or disable some streams."
            )
        if self.pool_size > self.BITGET_MAX_CONNECTIONS_PER_IP:
            raise ValueError(
                f"Bitget: Too many connections per IP (requested {self.pool_size}, "
                f"max allowed {self.BITGET_MAX_CONNECTIONS_PER_IP})"
            )
        if topics_per_socket > self.BITGET_RECOMMENDED_MAX_CHANNELS:
            logger.warning(
                f"Bitget: You have {topics_per_socket} channels per connection "
                f"(recommended <{self.BITGET_RECOMMENDED_MAX_CHANNELS} for stability)"
            )
        logger.info(
            f"Bitget limits validated: ~{topics_per_socket} topics/socket, "
            f"{symbols_per_socket} symbols/socket, {self.pool_size} websockets"
        )

    def _get_websocket_url(self, channel: Channel) -> str:
        """Return the Bitget WebSocket URL."""
        return "wss://ws.bitget.com/v2/ws/public"

    async def _subscribe(self, websocket: ClientConnection, symbols: set[str]) -> None:
        """Subscribe to streams for assigned symbols (including tickers)."""
        args: list[dict[str, str]] = []

        if self.trade_enabled:
            args += [
                {
                    "instType": self.inst_type,
                    "channel": "trade",
                    "instId": symbol,
                }
                for symbol in symbols
            ]
        if self.ticker_enabled:
            args += [
                {
                    "instType": self.inst_type,
                    "channel": "ticker",
                    "instId": symbol,
                }
                for symbol in symbols
            ]

        # Defensive: don't exceed per-connection channel limit in API call
        if len(args) > self.BITGET_MAX_CHANNELS_PER_CONN:
            raise ValueError(
                f"Bitget: Subscription would exceed "
                f"{self.BITGET_MAX_CHANNELS_PER_CONN} topics per connection "
                f"({len(args)} requested)"
            )

        if len(args) > self.BITGET_RECOMMENDED_MAX_CHANNELS:
            logger.warning(
                f"Bitget: Subscribing to {len(args)} streams in one connection. "
                f"For maximum reliability, "
                f"Bitget recommends <{self.BITGET_RECOMMENDED_MAX_CHANNELS}."
            )

        subscription_msg = {"op": "subscribe", "args": args}
        await websocket.send(orjson.dumps(subscription_msg).decode("utf-8"))
        logger.info(f"Subscribed to streams: {args} on channel {self.channel}")

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """
        Send periodic ping messages.

        - Sends "ping" every 30s.
        - Awaits for string "pong" response; if not received, reconnects.
        - Server disconnects after 2min without "ping".
        """
        try:
            while self._running:
                self._pong_received_event.clear()
                await websocket.send("ping")
                logger.debug(
                    f"Sent 'ping' on channel {self.channel} (sock {socket_id})"
                )
                try:
                    await asyncio.wait_for(self._pong_received_event.wait(), timeout=30)
                    logger.debug(
                        f"Received 'pong' on channel {self.channel} (sock {socket_id})"
                    )
                    # Received pong, continue loop
                except TimeoutError:
                    logger.error(
                        f"No 'pong' response from Bitget on {self.channel}, "
                        f"reconnecting socket {socket_id}"
                    )
                    # No pong received in time, close websocket to force reconnection
                    await websocket.close()
                    break
                await asyncio.sleep(30)
        except Exception as e:
            logger.error(
                f"Ping loop error on channel {self.channel} (socket {socket_id}): {e}"
            )

    async def _handle_message(self, message: Data, websocket: ClientConnection) -> None:
        """Handle incoming WebSocket message."""
        try:
            # Handle both binary and string (Bitget pings are raw string "pong")
            if isinstance(message, bytes):
                return

            # Server may send "pong" as plain string
            if message.lower() == "pong":
                logger.debug(f"Received string 'pong' on {self.channel}")
                self._pong_received_event.set()
                self._last_pong = asyncio.get_event_loop().time()
                return

            data = orjson.loads(message)

            if data.get("event") == "subscribe":
                logger.debug(
                    f"Subscription confirmed on channel {self.channel}: {data}"
                )
                return

            channel = data.get("arg", {}).get("channel")

            # Callbacks expect full message (for uniformity with other clients)
            if channel == "ticker":
                for ticker in data["data"]:
                    await self.on_ticker(ticker)
            elif channel == "trade":
                await self.on_trade(data)

        except orjson.JSONDecodeError as e:
            logger.error(
                f"Failed to parse message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
