"""Storage module for persisting Bybit tickers WebSocket event data in Redis.

This module provides a handler to process and persist incoming Bybit ticker events
directly to Redis under key bybit/tickers (as a Redis hash per symbol).
"""

import logging
from typing import Any, Dict, cast

import orjson
from redis.asyncio import Redis
from redis.asyncio import from_url as connection_from_url

from streamer.settings import settings

logger = logging.getLogger(__name__)

# Redis hash key for storing all tickers (per symbol as field)
REDIS_HASH_KEY = "bybit:tickers"
REDIS_MARK_PRICE_KEY = "bybit:mark-price"


class TickersStorage:
    """
    Storage handler to process incoming Bybit tickers events into Redis.

    Maintains local snapshots for each symbol; applies deltas and persists
    to Redis as a hash {symbol: serialized}.
    """

    def __init__(self) -> None:
        """Initialize the tickers storage handler and set up Redis connection."""
        self.redis: "Redis | None" = None
        self._is_connected: bool = False
        # local cache: symbol -> last snapshot dict
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._setup_on_init()

    def _setup_on_init(self) -> None:
        """
        Set up Redis connection and resources during initialization (sync).

        This is run from __init__ (not async). Redis connection object can be created,
        but will actually open socket only on the first command.
        """
        if not settings.redis_url:
            raise ValueError("REDIS_URL is required when tickers storage is enabled")
        try:
            self.redis = connection_from_url(  # type: ignore[no-untyped-call]
                settings.redis_url,
                decode_responses=False,
            )
            self._is_connected = True
            logger.info(
                f"Connected to Redis at {settings.redis_url} for tickers storage"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Redis for tickers storage: {e}")
            raise

    async def handle_event(self, data: Dict[str, Any]) -> None:
        """Apply snapshots and deltas, and save ticker and mark price."""
        if not settings.storage_enabled:
            return

        redis = self.redis
        if redis is None:
            logger.error("TickersStorage: Redis connection is not initialized")
            return

        topic = data.get("topic")
        if not (isinstance(topic, str) and topic.startswith("tickers.")):
            return

        try:
            symbol = topic.split(".", 1)[1]
        except Exception:
            symbol = "unknown"

        typ = data.get("type")
        tickers_data = cast("dict[str, Any]", data.get("data"))
        if not tickers_data:
            logger.warning(f"No 'data' field in tickers event: {data}")
            return

        cache = self._snapshots

        # Обработка снапшота или дельты
        if typ == "snapshot":
            cache[symbol] = tickers_data
            snapshot = tickers_data
        elif typ == "delta":
            item = cache.get(symbol)
            if item is not None:
                item.update(tickers_data)
                snapshot = item
            else:
                cache[symbol] = tickers_data
                snapshot = tickers_data
        else:
            snapshot = tickers_data

        # Сохраняем в одной redis-транзакции
        try:
            mark_price = snapshot["markPrice"]
            async with redis.pipeline() as pipe:
                pipe.hset(
                    REDIS_HASH_KEY,
                    symbol,
                    orjson.dumps(snapshot).decode("utf-8"),
                )

                pipe.hset(
                    REDIS_MARK_PRICE_KEY,
                    symbol,
                    str(mark_price),
                )
                await pipe.execute()
        except Exception as e:
            logger.error(
                f"Failed to store tickers data for symbol '{symbol}' in Redis: {e}"
            )

    async def close(self) -> None:
        """Close Redis connection if open."""
        if self.redis:
            try:
                await self.redis.aclose()
                logger.info("Tickers storage Redis connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
            finally:
                self.redis = None
                self._is_connected = False
