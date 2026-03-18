"""Kucoin WebSocket client."""

import asyncio
import logging
import time
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

import aiohttp
import orjson
from websockets import Data
from websockets.asyncio.client import ClientConnection

from streamer.settings import settings
from streamer.types import Channel
from streamer.websockets.base import WebSocketClient

logger = logging.getLogger(__name__)

KUCOIN_MAX_SUBSCRIPTION_BATCH_SIZE = 100  # Kucoin's documented max per subscribe


class KucoinWebSocketClient(WebSocketClient):
    """
    Kucoin WebSocket client for contract/futures ticker channels.

    Only `/contractMarket/ticker:{symbol}` (Ticker V1) is supported.
    """

    FUTURES_WS_TOKEN_ENDPOINT = "https://api-futures.kucoin.com/api/v1/bullet-public"  # noqa: S105

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Init Kucoin client."""
        super().__init__(channel, on_trade, on_ticker, on_symbols_count_changed)

        if not (
            settings.enable_ticker_stream
            or settings.enable_price_stream
            or settings.enable_tickers_kline_stream
        ):
            raise RuntimeError(
                "No supported stream enabled for Kucoin "
                "(need ticker/price/tickers_kline)"
            )

        self._token_info: dict[str, Any] = {}
        self._ping_interval = 18  # default, will be overwritten by token info
        self._ping_timeout = 10
        self._connect_id: str | None = None

    async def _fetch_ws_token(self) -> dict[str, Any]:
        """Fetch WS token and endpoint from Kucoin REST."""
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                self.FUTURES_WS_TOKEN_ENDPOINT, timeout=aiohttp.ClientTimeout(10)
            ) as resp,
        ):
            resp.raise_for_status()
            data = await resp.text()
        token_data = orjson.loads(data)
        if not token_data["code"] == "200000":
            raise RuntimeError(f"Failed to fetch Kucoin WS token: {token_data}")
        return token_data["data"]  # type: ignore[no-any-return]

    def _generate_connect_id(self) -> str:
        """Return a unique connectId string for WS connection."""
        return uuid.uuid4().hex[:10]

    async def _get_websocket_url(self, channel: Channel) -> str:
        """Get websocket URL using token API."""
        self._token_info = await self._fetch_ws_token()
        instance = self._token_info["instanceServers"][0]
        ws_base = instance["endpoint"].replace("https://", "wss://")
        token = self._token_info["token"]
        self._ping_interval = instance.get("pingInterval", 18000) // 1000
        self._ping_timeout = instance.get("pingTimeout", 10000) // 1000
        self._connect_id = self._generate_connect_id()
        return f"{ws_base}?token={token}&connectId={self._connect_id}"

    @staticmethod
    def _chunked(iterable: set[str], size: int) -> list[list[str]]:
        """Split a set of strings into batches/lists of the given size."""
        items = list(iterable)
        return [items[i : i + size] for i in range(0, len(items), size)]

    async def _subscribe(self, websocket: ClientConnection, symbols: set[str]) -> None:
        """Subscribe to /contractMarket/ticker:{symbol} for each contract symbol."""
        if not symbols:
            return
        symbol_batches = self._chunked(symbols, KUCOIN_MAX_SUBSCRIPTION_BATCH_SIZE)
        for batch_num, batch in enumerate(symbol_batches, start=1):
            topic_symbols = ",".join(batch)
            topic = f"/contractMarket/ticker:{topic_symbols}"
            msg_id = str(int(time.time() * 1000))
            subscription_msg = {
                "id": msg_id,
                "type": "subscribe",
                "topic": topic,
                "privateChannel": False,
                "response": True,
            }
            await websocket.send(orjson.dumps(subscription_msg).decode("utf-8"))
            logger.info(
                f"Subscribed to Kucoin /contractMarket/ticker (V1) for "
                f"batch {batch_num}/{len(symbol_batches)}: "
                f"{batch} on channel {self.channel}"
            )
            # Kucoin recommends not sending multiple subscription requests too rapidly
            if batch_num != len(symbol_batches):
                await asyncio.sleep(0.1)  # small delay between batches

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send ping messages at the required interval."""
        while self._running:
            try:
                msg_id = str(int(time.time() * 1000))
                ping_msg = {"id": msg_id, "type": "ping"}
                await websocket.send(orjson.dumps(ping_msg).decode("utf-8"))
                await asyncio.sleep(self._ping_interval)
            except Exception as e:
                logger.error(
                    f"Ping error on channel {self.channel} (socket {socket_id}): {e}"
                )
                break

    async def _handle_message(self, message: Data, websocket: ClientConnection) -> None:
        """Process Kucoin WebSocket message for ticker V1 topic."""
        try:
            data = orjson.loads(message)
            msg_type = data["type"]
            if msg_type == "welcome":
                logger.info(f"WebSocket connection established: {data}")
                return
            if msg_type == "ack":
                logger.debug(f"Subscription ACK on channel {self.channel}: {data}")
                return
            if msg_type == "pong":
                logger.debug(f"Pong received on channel {self.channel}: {data}")
                return
            if msg_type == "ping":
                pong_msg = {
                    "id": data["id"],
                    "type": "pong",
                    "timestamp": int(time.time() * 1000000),
                }
                await websocket.send(orjson.dumps(pong_msg).decode("utf-8"))
                logger.debug(f"Responded to server ping on {self.channel}")
                return
            if msg_type == "error":
                logger.error(f"Error from Kucoin WebSocket {self.channel}: {data}")
                return
            if msg_type == "message" and data["subject"] == "ticker":
                await self.on_ticker(data)
                return
            logger.warning(f"Ignored message on channel {self.channel}: {data}")

        except orjson.JSONDecodeError as e:
            logger.error(
                f"Failed to parse message on channel {self.channel}: {e}, "
                f"message: {message!s}"
            )
