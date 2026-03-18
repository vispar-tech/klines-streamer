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
from streamer.websockets.kucoin import KucoinWebSocketClient
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
    match settings.exchange:
        case "bingx":
            return BingxWebSocketClient(**kwargs)
        case "bybit":
            return BybitWebSocketClient(**kwargs)
        case "bitget":
            return BitgetWebSocketClient(**kwargs)
        case "okx":
            return OkxWebSocketClient(**kwargs)
        case "binance":
            return BinanceWebSocketClient(**kwargs)
        case "kucoin":
            return KucoinWebSocketClient(**kwargs)
