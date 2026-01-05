"""Symbol loading and validation for Bybit exchange."""

import logging
from typing import Any

import aiohttp
import orjson
from aiohttp import ClientTimeout

from streamer.settings import settings

logger = logging.getLogger(__name__)

BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_CATEGORY = "linear"
BYBIT_STATUS = "Trading"
BYBIT_MAX_PAGE_LIMIT = 1000  # max limit per docs


async def load_all_symbols(limit: int | None = None) -> set[str]:
    """
    Load all trading symbols from Bybit API (with pagination).

    Args:
        limit: Optional limit on number of symbols to return

    Returns:
        Set of trading symbol names
    """
    result_symbols: list[str] = []
    next_cursor: str | None = None
    total_seen = 0

    try:
        timeout = ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                params = {
                    "category": BYBIT_CATEGORY,
                    "status": BYBIT_STATUS,
                    "limit": str(BYBIT_MAX_PAGE_LIMIT),
                }
                if next_cursor:
                    params["cursor"] = next_cursor

                async with session.get(
                    BYBIT_INSTRUMENTS_URL, params=params
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
                new_symbols = [item["symbol"] for item in sorted_items]
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


async def validate_symbols(symbols: set[str]) -> set[str]:
    """
    Validate symbols against Bybit API and return valid ones as a set.

    Args:
        symbols: List of symbols to validate

    Returns:
        Set of valid symbols (invalid ones are logged as warnings)
    """
    if not symbols:
        return set()

    try:
        available_symbols = await load_all_symbols()
        valid_symbols: set[str] = set()

        for symbol in symbols:
            if symbol in available_symbols:
                valid_symbols.add(symbol)
            else:
                logger.warning(f"Symbol '{symbol}' is not available on Bybit, skipping")

        if len(valid_symbols) != len(symbols):
            logger.info(f"Validated {len(valid_symbols)}/{len(symbols)} symbols")

        return valid_symbols

    except Exception as exc:
        logger.warning(f"Failed to validate symbols, using original list: {exc}")
        return set(symbols)


async def validate_settings_symbols() -> None:
    """
    Process and validate symbols based on configuration.

    This should be called after settings initialization to handle async operations.
    """
    if settings.bybit_load_all_symbols:
        # Load all symbols from Bybit API
        settings.bybit_symbols = await load_all_symbols(settings.bybit_symbols_limit)
    else:
        # Validate provided symbols
        settings.bybit_symbols = await validate_symbols(settings.bybit_symbols)
