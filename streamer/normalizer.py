"""Module for normalizing and passing through trade and ticker data."""

from typing import Any, Dict

from streamer.settings import settings
from streamer.types import Channel

BINGX_FIELDS_MAP = {
    "e": "eventType",
    "E": "eventTime",
    "s": "symbol",  # Symbol, e.g. "BTC-USDT"
    "p": "priceChange24h",
    "P": "price24hPcnt",
    "o": "openPrice24h",
    "h": "highPrice24h",
    "l": "lowPrice24h",
    "L": "latestTradedVolume",
    "c": "currentPrice",
    "v": "volume24h",
    "q": "turnover24h",
    "O": "firstTradeTime24h",
    "C": "lastTradeTime24h",
    "B": "bid1Price",
    "b": "bid1Size",
    "A": "ask1Price",
    "a": "ask1Size",
}


class BaseNormalizer:
    """Base class for normalizing trade and ticker data."""

    def handle_trade(self, trade_data: Dict[str, Any], channel: Channel) -> Any:
        """Normalize and pass through trade data."""
        return trade_data

    def handle_ticker(self, ticker_data: Dict[str, Any], channel: Channel) -> Any:
        """Normalize and pass through ticker data."""
        return ticker_data


class BingxNormalizer(BaseNormalizer):
    """Normalizer for BingX."""

    def handle_trade(self, trade_data: Dict[str, Any], channel: Channel) -> Any:
        """Normalize BingX trade data."""
        for item in trade_data["data"]:
            if "q" in item:
                item["v"] = item.pop("q")
        return trade_data["data"]

    def handle_ticker(self, ticker_data: Dict[str, Any], channel: Channel) -> Any:
        """Normalize BingX ticker data."""
        data = ticker_data["data"]

        normalized_data: dict[str, Any] = {}

        for k, v in data.items():
            if k in BINGX_FIELDS_MAP:
                normalized_data[BINGX_FIELDS_MAP[k]] = v
            else:
                normalized_data[k] = v  # fallback, just in case

        topic = f"tickers.{data['s']}"
        ts = data["E"]

        # Set placeholder negative values to ensure expected structure;
        # these fields remain present and available
        normalized_data["openInterest"] = -1
        normalized_data["fundingRate"] = -1
        return {
            "topic": topic,
            "type": "snapshot",
            "data": normalized_data,
            "ts": ts,
        }


class BybitNormalizer(BaseNormalizer):
    """Normalizer for Bybit."""

    def handle_trade(self, trade_data: Dict[str, Any], channel: Channel) -> Any:
        """Normalize Bybit trade data."""
        return trade_data["data"]

    def handle_ticker(self, ticker_data: Dict[str, Any], channel: Channel) -> Any:
        """Normalize Bybit ticker data."""
        if channel == "linear" and "markPrice" in ticker_data["data"]:
            ticker_data["data"]["currentPrice"] = ticker_data["data"].pop("markPrice")
        elif channel == "spot" and "lastPrice" in ticker_data["data"]:
            ticker_data["data"]["currentPrice"] = ticker_data["data"].pop("lastPrice")

        return ticker_data


def get_normalizer() -> BaseNormalizer:
    """Return the appropriate normalizer based on exchange setting."""
    if settings.exchange == "bingx":
        return BingxNormalizer()
    if settings.exchange == "bybit":
        return BybitNormalizer()
    raise RuntimeError(f"No normalizer found for exchange: {settings.exchange}")


# Singleton instance based on current settings
normalizer: BaseNormalizer = get_normalizer()
