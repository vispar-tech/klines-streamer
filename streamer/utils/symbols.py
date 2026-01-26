"""Unified symbol loading and validation for different exchanges."""

import logging
from abc import ABC, abstractmethod
from typing import Any

import aiohttp
import orjson
from aiohttp import ClientTimeout

from streamer.settings import settings

logger = logging.getLogger(__name__)


class BaseSymbolLoader(ABC):
    """Base class for loading symbols from different exchanges."""

    def __init__(self, exchange_name: str) -> None:
        """Initialize symbol loader for specific exchange."""
        self.exchange_name = exchange_name

    @abstractmethod
    async def load_all_symbols(self, limit: int | None = None) -> set[str]:
        """Load all trading symbols from exchange API."""

    async def validate_symbols(self, symbols: set[str]) -> set[str]:
        """
        Validate symbols against exchange API and return valid ones.

        Args:
            symbols: Set of symbols to validate

        Returns:
            Set of valid symbols (invalid ones are logged as warnings)
        """
        if not symbols:
            return set()

        try:
            available_symbols = await self.load_all_symbols()
            valid_symbols: set[str] = set()

            for symbol in symbols:
                if symbol in available_symbols:
                    valid_symbols.add(symbol)
                else:
                    logger.warning(
                        f"Symbol '{symbol}' is not available "
                        f"on {self.exchange_name}, skipping"
                    )

            if len(valid_symbols) != len(symbols):
                logger.info(f"Validated {len(valid_symbols)}/{len(symbols)} symbols")

            return valid_symbols

        except Exception as exc:
            logger.warning(
                f"Failed to validate symbols on {self.exchange_name}, "
                f"using original list: {exc}"
            )
            return set(symbols)


class BybitSymbolLoader(BaseSymbolLoader):
    """Symbol loader for Bybit exchange."""

    def __init__(self) -> None:
        """Initialize Bybit symbol loader."""
        super().__init__("Bybit")
        self.instruments_url = "https://api.bybit.com/v5/market/instruments-info"
        self.category = "linear"
        self.status = "Trading"
        self.max_page_limit = 1000

    async def load_all_symbols(self, limit: int | None = None) -> set[str]:
        """Load all trading symbols from Bybit API (with pagination)."""
        result_symbols: list[str] = []
        next_cursor: str | None = None
        total_seen = 0

        try:
            timeout = ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    params = {
                        "category": self.category,
                        "status": self.status,
                        "limit": str(self.max_page_limit),
                    }
                    if next_cursor:
                        params["cursor"] = next_cursor

                    async with session.get(
                        self.instruments_url, params=params
                    ) as response:
                        response.raise_for_status()
                        data = orjson.loads(await response.read())

                    items = data.get("result", {}).get("list", [])
                    valid_items: list[dict[str, Any]] = [
                        item
                        for item in items
                        if isinstance(item, dict)
                        and "symbol" in item
                        and "launchTime" in item
                    ]
                    sorted_items = sorted(valid_items, key=lambda x: x["launchTime"])
                    new_symbols = [
                        str(item["symbol"])
                        for item in sorted_items
                        if str(item["symbol"]).endswith("USDT")
                    ]
                    result_symbols.extend(new_symbols)
                    total_seen += len(new_symbols)

                    # Pagination: check if nextPageCursor/cursor is present
                    next_cursor = data.get("result", {}).get("nextPageCursor")
                    # Respect user limit if passed
                    if limit is not None and total_seen >= limit:
                        break
                    if not next_cursor:
                        break  # End of pages

            # Enforce limit, uniqueness
            unique_syms: list[str] = []
            seen_set: set[str] = set()
            for s in result_symbols:
                if s not in seen_set:
                    unique_syms.append(s)
                    seen_set.add(s)
                if limit is not None and len(unique_syms) >= limit:
                    break

            logger.info(f"Loaded {len(unique_syms)} symbols from Bybit API")
            return set(unique_syms)

        except Exception as exc:
            logger.warning(f"Failed to load symbols from Bybit API: {exc}")
            return set()


class BingxSymbolLoader(BaseSymbolLoader):
    """Symbol loader for BingX exchange."""

    def __init__(self) -> None:
        """Initialize BingX symbol loader."""
        super().__init__("BingX")
        self.contracts_url = (
            "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
        )

    async def load_all_symbols(self, limit: int | None = None) -> set[str]:
        """Load all trading symbols from BingX API."""
        try:
            timeout = ClientTimeout(total=20)
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(self.contracts_url) as response,
            ):
                response.raise_for_status()
                data = orjson.loads(await response.read())

            instruments: list[dict[str, Any]] = data.get("data", [])
            symbols = [
                str(item.get("symbol", ""))
                for item in instruments
                if item.get("status") == 1 and item.get("currency") == "USDT"
            ]

            # Filter out empty symbols
            symbols = [s for s in symbols if s]

            # Apply limit if specified
            if limit is not None:
                symbols = symbols[:limit]

            logger.info(f"Loaded {len(symbols)} symbols from BingX API")
            return set(symbols)

        except Exception as exc:
            logger.warning(f"Failed to load symbols from BingX API: {exc}")
            return set()


class BitgetSymbolLoader(BaseSymbolLoader):
    """Symbol loader for Bitget exchange (USDT-M contracts)."""

    def __init__(self) -> None:
        """Initialize Bitget symbol loader."""
        super().__init__("Bitget")
        self.contracts_url = "https://api.bitget.com/api/v2/mix/market/contracts"
        self.product_type = "USDT-FUTURES"

    async def load_all_symbols(self, limit: int | None = None) -> set[str]:
        """Load all trading symbols from Bitget API (USDT-FUTURES contracts)."""
        try:
            timeout = ClientTimeout(total=20)
            params = {
                "productType": self.product_type,
            }
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(self.contracts_url, params=params) as response,
            ):
                response.raise_for_status()
                data = orjson.loads(await response.read())
            # Expect: data["code"] == "00000", data["data"] = list of contracts
            contracts: list[dict[str, Any]] = data.get("data", [])
            symbols = [
                str(item["symbol"])
                for item in contracts
                if item.get("symbolStatus") == "normal"
                and str(item.get("quoteCoin", "")) == "USDT"
            ]
            symbols = [s for s in symbols if s]

            if limit is not None:
                symbols = symbols[:limit]

            logger.info(f"Loaded {len(symbols)} symbols from Bitget API")
            return set(symbols)
        except Exception as exc:
            logger.warning(f"Failed to load symbols from Bitget API: {exc}")
            return set()


def get_symbol_loader() -> BaseSymbolLoader:
    """Return the appropriate symbol loader based on exchange setting."""
    if settings.exchange == "bingx":
        return BingxSymbolLoader()
    if settings.exchange == "bybit":
        return BybitSymbolLoader()
    if settings.exchange == "bitget":
        return BitgetSymbolLoader()
    # Default fallback
    raise ValueError(f"Unsupported exchange for symbol loading: {settings.exchange!r}")


# Singleton instance based on current settings
symbol_loader: BaseSymbolLoader = get_symbol_loader()


async def load_all_symbols(limit: int | None = None) -> set[str]:
    """Load all trading symbols from the current exchange."""
    return await symbol_loader.load_all_symbols(limit)


async def validate_symbols(symbols: set[str]) -> set[str]:
    """Validate symbols against the current exchange."""
    return await symbol_loader.validate_symbols(symbols)


async def validate_settings_symbols() -> None:
    """
    Process and validate symbols based on configuration.

    This should be called after settings initialization to handle async operations.
    """
    if settings.exchange_load_all_symbols:
        # Load all symbols from exchange API
        settings.exchange_symbols = await load_all_symbols(
            settings.exchange_symbols_limit
        )
    else:
        # Validate provided symbols
        settings.exchange_symbols = await validate_symbols(settings.exchange_symbols)
