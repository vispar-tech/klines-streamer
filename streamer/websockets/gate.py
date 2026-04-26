"""Gate WebSocket client implementation."""

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


class GateWebSocketClient(WebSocketClient):
    """Gate WebSocket client with socket pool support."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Initialize Gate WebSocket client."""
        super().__init__(channel, on_trade, on_ticker, on_symbols_count_changed)

        # index-tickers only, no trade nor klines supported!
        self._index_tickers_enabled = (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        )

        if settings.enable_klines_stream:
            raise RuntimeError("Currently GATE does not support klines from trades")
        if not self._index_tickers_enabled:
            raise RuntimeError(
                "No supported stream enabled for GATE (need ticker/price/tickers_kline)"
            )

    async def _get_websocket_url(self, channel: Channel) -> str:
        """Return the Gate WebSocket URL for futures tickers."""
        return "wss://fx-ws.gateio.ws/v4/ws/usdt"

    async def _subscribe(self, websocket: ClientConnection, symbols: set[str]) -> None:
        """Subscribe to futures.tickers for the assigned symbols."""
        sub_msg = {
            "time": int(asyncio.get_event_loop().time()),
            "channel": "futures.tickers",
            "event": "subscribe",
            "payload": list(symbols),
        }
        await websocket.send(orjson.dumps(sub_msg).decode("utf-8"))
        logger.info(
            "Subscribed to GATE futures.tickers for: "
            f"{symbols} on channel {self.channel}"
        )

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""
        while self._running:
            try:
                ping_msg = {
                    "time": int(asyncio.get_event_loop().time()),
                    "channel": "futures.ping",
                }
                await websocket.send(orjson.dumps(ping_msg).decode("utf-8"))
                await asyncio.sleep(25)
            except Exception as e:
                logger.error(
                    f"Ping error on channel {self.channel} (socket {socket_id}): {e}"
                )
                break

    async def _handle_message(self, message: Data, websocket: ClientConnection) -> None:
        """Handle incoming WebSocket message from GATE futures.tickers."""
        try:
            data = orjson.loads(message)

            channel = data["channel"]
            event = data.get("event")

            if channel == "futures.ping":
                pong_msg = {
                    "time": data["time"],
                    "channel": "futures.pong",
                    "event": "",
                    "result": None,
                }
                await websocket.send(orjson.dumps(pong_msg).decode("utf-8"))
                logger.debug(
                    "Received futures.ping, "
                    f"responded with futures.pong for channel {self.channel}"
                )
                return

            if channel == "futures.pong":
                return

            # Confirm subscription
            if channel == "futures.tickers" and event == "subscribe":
                result = data["result"]
                if isinstance(result, dict) and result["status"] == "success":
                    logger.info(
                        "Subscription confirmed for GATE "
                        f"futures.tickers ({self.channel}): {data}"
                    )
                else:
                    logger.warning(f"Unsuccessful subscribe result from GATE: {data}")
                return

            if event == "error":
                logger.error(
                    f"Error from GATE WebSocket for channel "
                    f"{self.channel}: {data['msg']}"
                )
                return

            if channel == "futures.tickers" and event == "update":
                tickers = data["result"]
                for ticker in tickers:
                    await self.on_ticker(ticker)
                return

            return

        except orjson.JSONDecodeError as e:
            logger.error(
                f"Failed to parse message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
