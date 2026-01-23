"""Factory functions for WebSocket clients."""

from typing import Any, Callable, Coroutine, Dict

from streamer.settings import settings
from streamer.types import Channel
from streamer.websockets.base import WebSocketClient
from streamer.websockets.bingx import BingxWebSocketClient
from streamer.websockets.bybit import BybitWebSocketClient


def get_websocket_client(
    channel: Channel,
    on_trade: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
    on_ticker: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
) -> WebSocketClient:
    """Return the appropriate WebSocket client based on exchange setting."""
    if settings.exchange == "bingx":
        return BingxWebSocketClient(channel, on_trade, on_ticker)
    if settings.exchange == "bybit":
        return BybitWebSocketClient(channel, on_trade, on_ticker)
    # Default fallback
    raise ValueError(f"Unsupported exchange: {settings.exchange!r}")
