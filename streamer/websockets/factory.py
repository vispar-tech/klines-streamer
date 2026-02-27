"""Factory functions for WebSocket clients."""

from collections.abc import Callable, Coroutine
from typing import Any

from streamer.settings import settings
from streamer.types import Channel
from streamer.websockets.base import WebSocketClient
from streamer.websockets.binance import BinanceWebSocketClient
from streamer.websockets.bingx import BingxWebSocketClient
from streamer.websockets.bitget import BitgetWebSocketClient
from streamer.websockets.bybit import BybitWebSocketClient
from streamer.websockets.okx import OkxWebSocketClient


def get_websocket_client(
    channel: Channel,
    on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
    on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
    on_symbols_count_changed: Callable[[int], None] | None = None,
) -> WebSocketClient:
    """Return the appropriate WebSocket client based on exchange setting."""
    kwargs: dict[str, Any] = {
        "channel": channel,
        "on_trade": on_trade,
        "on_ticker": on_ticker,
        "on_symbols_count_changed": on_symbols_count_changed,
    }
    if settings.exchange == "bingx":
        return BingxWebSocketClient(**kwargs)
    if settings.exchange == "bybit":
        return BybitWebSocketClient(**kwargs)
    if settings.exchange == "bitget":
        return BitgetWebSocketClient(**kwargs)
    if settings.exchange == "okx":
        return OkxWebSocketClient(**kwargs)
    if settings.exchange == "binance":
        return BinanceWebSocketClient(**kwargs)
    raise ValueError(f"Unsupported exchange: {settings.exchange!r}")
