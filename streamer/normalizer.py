"""Module for normalizing and passing through trade and ticker data."""

from typing import Any

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


BITGET_FIELDS_MAP = {
    "lastPr": "lastPr",
    "symbol": "symbol",
    "indexPrice": "indexPrice",
    "open24h": "openPrice24h",
    "nextFundingTime": "nextFundingTime",
    "bidPr": "bid1Price",
    "change24h": "price24hPcnt",
    "quoteVolume": "turnover24h",
    "deliveryPrice": "deliveryPrice",
    "askSz": "ask1Size",
    "low24h": "lowPrice24h",
    "symbolType": "symbolType",
    "openUtc": "openPriceUTC",
    "instId": "symbol",  # This sometimes mirrors 'symbol'
    "bidSz": "bid1Size",
    "markPrice": "currentPrice",
    "high24h": "highPrice24h",
    "askPr": "ask1Price",
    "holdingAmount": "openInterest",
    "baseVolume": "volume24h",
    "fundingRate": "fundingRate",
    "ts": "eventTime",
}

BINANCE_FIELDS_MAP = {
    "e": "eventType",  # Event type, e.g. "24hrTicker"
    "E": "eventTime",  # Event time
    "s": "symbol",  # Symbol, e.g. "BTCUSDT"
    "p": "priceChange24h",  # Price change
    "P": "price24hPcnt",  # Price change percent
    "w": "weightedAvgPrice",  # Weighted average price
    "c": "currentPrice",  # Last (current) price
    "Q": "lastQuantity",  # Last quantity
    "o": "openPrice24h",  # Open price
    "h": "highPrice24h",  # High price
    "l": "lowPrice24h",  # Low price
    "v": "volume24h",  # Total traded base asset volume
    "q": "turnover24h",  # Total traded quote asset volume
    "O": "firstTradeTime24h",  # Statistics open time
    "C": "lastTradeTime24h",  # Statistics close time
    "F": "firstTradeId",  # First trade ID
    "L": "lastTradeId",  # Last trade Id
    "n": "tradesCount24h",  # Total number of trades
}

OKX_INDEX_TICKERS_FIELDS_MAP = {
    "instId": "symbol",  # Index, e.g. BTC-USDT
    "idxPx": "currentPrice",  # Latest Index Price (string)
    "open24h": "openPrice24h",  # Open price in the past 24 hours (string)
    "high24h": "highPrice24h",  # Highest price in the past 24 hours (string)
    "low24h": "lowPrice24h",  # Lowest price in the past 24 hours (string)
    "sodUtc0": "openPriceUTC0",  # Open price in the UTC 0 (string)
    "sodUtc8": "openPriceUTC8",  # Open price in the UTC 8 (string)
    "ts": "eventTime",  # Update time, Unix ms
}


class BaseNormalizer:
    """Base class for normalizing trade and ticker data."""

    def handle_trade(
        self, trade_data: dict[str, Any], channel: Channel
    ) -> list[dict[str, Any]] | None:
        """Normalize and pass through trade data."""
        raise NotImplementedError("handle_trade is not implemented")

    def handle_ticker(
        self, ticker_data: dict[str, Any], channel: Channel
    ) -> dict[str, Any] | None:
        """Normalize and pass through ticker data."""
        raise NotImplementedError("handle_ticker is not implemented")


class BingxNormalizer(BaseNormalizer):
    """Normalizer for BingX."""

    def handle_trade(
        self, trade_data: dict[str, Any], channel: Channel
    ) -> list[dict[str, Any]] | None:
        """Normalize BingX trade data."""
        for item in trade_data["data"]:
            if "q" in item:
                item["v"] = item.pop("q")
        return trade_data["data"]  # type: ignore[no-any-return]

    def handle_ticker(
        self, ticker_data: dict[str, Any], channel: Channel
    ) -> dict[str, Any] | None:
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

    def handle_trade(
        self, trade_data: dict[str, Any], channel: Channel
    ) -> list[dict[str, Any]] | None:
        """Normalize Bybit trade data."""
        return trade_data["data"]  # type: ignore[no-any-return]

    def handle_ticker(
        self, ticker_data: dict[str, Any], channel: Channel
    ) -> dict[str, Any] | None:
        """Normalize Bybit ticker data."""
        if channel == "linear" and "markPrice" in ticker_data["data"]:
            ticker_data["data"]["currentPrice"] = ticker_data["data"].pop("markPrice")
        elif channel == "spot" and "lastPrice" in ticker_data["data"]:
            ticker_data["data"]["currentPrice"] = ticker_data["data"].pop("lastPrice")

        return ticker_data


class BitgetNormalizer(BaseNormalizer):
    """Normalizer for Bitget."""

    def handle_trade(
        self, trade_data: dict[str, Any], channel: Channel
    ) -> list[dict[str, Any]] | None:
        """If trade normalization is needed, implement it here."""
        # For now, just pass through the data list
        return [
            {
                "T": trade["ts"],
                "s": trade_data["arg"]["instId"],
                "p": trade["price"],
                "v": trade["size"],
            }
            for trade in trade_data["data"]
        ]

    def handle_ticker(
        self, ticker_data: dict[str, Any], channel: Channel
    ) -> dict[str, Any] | None:
        """Normalize Bitget ticker data."""
        normalized_data: dict[str, Any] = {}

        for k, v in ticker_data.items():
            if k in BITGET_FIELDS_MAP:
                normalized_data[BITGET_FIELDS_MAP[k]] = v
            else:
                normalized_data[k] = v  # fallback, just in case

        symbol = ticker_data["symbol"]
        topic = f"tickers.{symbol}"

        ts = ticker_data["ts"]

        return {
            "topic": topic,
            "type": "snapshot",
            "data": normalized_data,
            "ts": ts,
        }


class OkxNormalizer(BaseNormalizer):
    """Normalizer for Okx."""

    def handle_trade(
        self, trade_data: dict[str, Any], channel: Channel
    ) -> list[dict[str, Any]] | None:
        """If trade normalization is needed, implement it here."""
        # todo
        return None

    def handle_ticker(
        self, ticker_data: dict[str, Any], channel: Channel
    ) -> dict[str, Any] | None:
        """Normalize ticker data."""
        normalized_data: dict[str, Any] = {}

        for k, v in ticker_data.items():
            if k in OKX_INDEX_TICKERS_FIELDS_MAP:
                normalized_data[OKX_INDEX_TICKERS_FIELDS_MAP[k]] = v
            else:
                normalized_data[k] = v  # fallback, just in case

        symbol = ticker_data["instId"]
        topic = f"tickers.{symbol}"

        ts = ticker_data["ts"]

        return {
            "topic": topic,
            "type": "snapshot",
            "data": normalized_data,
            "ts": ts,
        }


class BinanceNormalizer(BaseNormalizer):
    """Normalizer for Binance."""

    def handle_trade(
        self, trade_data: dict[str, Any], channel: Channel
    ) -> list[dict[str, Any]] | None:
        """If trade normalization is needed, implement it here."""
        # todo
        return None

    def handle_ticker(
        self, ticker_data: dict[str, Any], channel: Channel
    ) -> dict[str, Any] | None:
        """Normalize ticker data."""
        normalized_data: dict[str, Any] = {}

        for k, v in ticker_data.items():
            if k in BINANCE_FIELDS_MAP:
                normalized_data[BINANCE_FIELDS_MAP[k]] = v
            else:
                normalized_data[k] = v  # fallback, just in case

        symbol = ticker_data["s"]
        topic = f"tickers.{symbol}"

        ts = ticker_data["E"]

        return {
            "topic": topic,
            "type": "snapshot",
            "data": normalized_data,
            "ts": ts,
        }


def get_normalizer() -> BaseNormalizer:
    """Return the appropriate normalizer based on exchange setting."""
    if settings.exchange == "bingx":
        return BingxNormalizer()
    if settings.exchange == "bybit":
        return BybitNormalizer()
    if settings.exchange == "bitget":
        return BitgetNormalizer()
    if settings.exchange == "okx":
        return OkxNormalizer()
    if settings.exchange == "binance":
        return BinanceNormalizer()
    raise RuntimeError(f"No normalizer found for exchange: {settings.exchange}")


# Singleton instance based on current settings
normalizer: BaseNormalizer = get_normalizer()
