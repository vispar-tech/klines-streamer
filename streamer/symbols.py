"""Symbol loading and validation for Bybit exchange."""

import logging
from typing import Any

import aiohttp
import orjson
from aiohttp import ClientTimeout

logger = logging.getLogger(__name__)

BYBIT_INSTRUMENTS_URL = (
    "https://api.bybit.com/v5/market/instruments-info?category=linear&status=Trading"
)


async def load_all_symbols(limit: int | None = None) -> set[str]:
    """
    Load all trading symbols from Bybit API.

    Args:
        limit: Optional limit on number of symbols to return

    Returns:
        Set of trading symbol names
    """
    try:
        timeout = ClientTimeout(total=10)
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.get(BYBIT_INSTRUMENTS_URL) as response,
        ):
            response.raise_for_status()
            data = orjson.loads(await response.read())

        # Extract and sort symbols by launch time
        items = data.get("result", {}).get("list", [])
        valid_items: list[dict[str, Any]] = [
            item
            for item in items
            if isinstance(item, dict) and "symbol" in item and "launchTime" in item
        ]
        sorted_items = sorted(valid_items, key=lambda x: x["launchTime"])
        symbols = {item["symbol"] for item in sorted_items}

        if limit:
            symbols = set(list(symbols)[:limit])

        logger.info(f"Loaded {len(symbols)} symbols from Bybit API")
        return symbols

    except Exception as exc:
        logger.warning(f"Failed to load symbols from Bybit API: {exc}")
        return set()


async def validate_symbols(symbols: list[str]) -> list[str]:
    """
    Validate symbols against Bybit API and return valid ones.

    Args:
        symbols: List of symbols to validate

    Returns:
        List of valid symbols (invalid ones are logged as warnings)
    """
    if not symbols:
        return []

    try:
        available_symbols = await load_all_symbols()
        valid_symbols: list[str] = []

        for symbol in symbols:
            if symbol in available_symbols:
                valid_symbols.append(symbol)
            else:
                logger.warning(f"Symbol '{symbol}' is not available on Bybit, skipping")

        if len(valid_symbols) != len(symbols):
            logger.info(f"Validated {len(valid_symbols)}/{len(symbols)} symbols")

        return valid_symbols

    except Exception as exc:
        logger.warning(f"Failed to validate symbols, using original list: {exc}")
        return symbols
