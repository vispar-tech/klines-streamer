"""Storage module for persisting Bybit tickers WebSocket event data in Redis.

This module provides a handler to process and persist incoming Bybit ticker events
directly to Redis under key bybit/tickers (as a Redis hash per symbol).
Also broadcasts every ticker snapshot to the REDIS_HASH_KEY pubsub channel.
"""

import logging
from typing import Any, Dict

import orjson
from redis.asyncio import Redis
from redis.asyncio import from_url as connection_from_url

from streamer.settings import settings
from streamer.types import Channel, Interval

logger = logging.getLogger(__name__)

# Redis hash key for storing all tickers (per symbol as field)
REDIS_MAIN_KEY = f"{settings.redis_main_key}:{settings.exchange}"
REDIS_HASH_KEY = f"{REDIS_MAIN_KEY}:tickers"
REDIS_PRICE_KEY = f"{REDIS_MAIN_KEY}:price"
REDIS_LAST_CLOSED = f"{REDIS_MAIN_KEY}:last-closed"
REDIS_LAST_CLOSED_TICKERS = f"{REDIS_MAIN_KEY}:last-closed-tickers"


class Storage:
    """
    Storage handler to process incoming Bybit tickers events into Redis.

    Maintains local snapshots for each symbol; applies deltas and persists
    to Redis as a hash {symbol: serialized}. Also broadcasts each snapshot to pubsub.
    """

    def __init__(self) -> None:
        """Initialize the tickers storage handler and set up Redis connection."""
        self.redis: "Redis | None" = None
        self._is_connected: bool = False

    async def setup(self) -> None:
        """
        Set up Redis connection and resources during initialization (sync).

        This is run from __init__ (not async). Redis connection object can be created,
        but will actually open socket only on the first command.
        """
        if not settings.redis_url:
            raise ValueError("REDIS_URL is required when storage is enabled")

        try:
            self.redis = connection_from_url(  # type: ignore[no-untyped-call]
                settings.redis_url,
                decode_responses=False,
            )
            self._is_connected = True
            logger.info(f"Connected to Redis at {settings.redis_url} for storage")
            # Flush all relevant Redis keys for a clean start
            try:
                # Delete all keys under the "bybit-streamer" prefix in Redis
                async for key in self.redis.scan_iter(match=f"{REDIS_MAIN_KEY}*"):
                    await self.redis.delete(key)
                logger.info(
                    f"Deleted all Redis keys: {REDIS_HASH_KEY},"
                    f" {REDIS_PRICE_KEY} for clean initialization"
                )
            except Exception as e:
                logger.warning(f"Failed to delete storage Redis keys during setup: {e}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis for storage: {e}")
            raise

    async def process_ticker(
        self,
        channel: Channel,
        symbol: str,
        snapshot: Dict[str, Any],
        price: float,
    ) -> None:
        """Broadcast ticker, store in Redis, and notify pubsub."""
        redis = self.redis
        if redis is None:
            logger.error("Storage: Redis connection is not initialized")
            return

        # Now write everything in redis
        await self._save_snapshot_to_redis(channel, symbol, snapshot, price)

    async def _save_snapshot_to_redis(
        self, channel: str, symbol: str, snapshot: dict[str, Any], price: float
    ) -> None:
        """Persist the parsed tickers snapshot and price into Redis."""
        redis = self.redis
        if redis is None:
            logger.error("Storage: Redis connection is not initialized")
            return

        try:
            async with redis.pipeline() as pipe:
                pipe.hset(
                    f"{REDIS_HASH_KEY}:{channel}",
                    symbol,
                    orjson.dumps(snapshot).decode("utf-8"),
                )
                pipe.hset(
                    f"{REDIS_PRICE_KEY}:{channel}",
                    symbol,
                    str(price),
                )
                await pipe.execute()
        except Exception as e:
            logger.error(
                f"Failed to store tickers data for symbol '{symbol}' in Redis: {e}"
            )

    async def process_klines(
        self, channel: Channel, data: list[dict[str, Any]]
    ) -> None:
        """
        Store kline data into Redis, grouped by interval.

        Each interval has its own key under the channel.
        """
        redis = self.redis
        if redis is None:
            logger.error("Storage: Redis connection is not initialized")
            return

        # Group klines by interval (assume "interval" field exists in each kline)
        intervals_map: dict[str, list[dict[str, Any]]] = {}
        for kline in data:
            intervals_map.setdefault(kline["interval"], []).append(kline)

        try:
            for interval, grouped_klines in intervals_map.items():
                parsed_interval = Interval(interval)
                key = f"{REDIS_LAST_CLOSED}:{channel}:{parsed_interval}"
                await redis.set(
                    key,
                    orjson.dumps(grouped_klines).decode("utf-8"),
                    px=parsed_interval.to_milliseconds(),
                )
        except Exception as e:
            logger.error(f"Failed to save klines to Redis (channel={channel}): {e}")

    async def process_tickers_klines(
        self, channel: Channel, data: list[dict[str, Any]]
    ) -> None:
        """
        Store ticker kline data into Redis, grouped by interval.

        Each interval has its own key under the channel with ticker kline prefix.
        """
        redis = self.redis
        if redis is None:
            logger.error("Storage: Redis connection is not initialized")
            return

        # Group ticker klines by interval (assume "interval" field exists in each kline)
        intervals_map: dict[str, list[dict[str, Any]]] = {}
        for kline in data:
            intervals_map.setdefault(kline["interval"], []).append(kline)

        try:
            for interval, grouped_klines in intervals_map.items():
                parsed_interval = Interval(interval)
                key = f"{REDIS_LAST_CLOSED_TICKERS}:{channel}:{parsed_interval}"
                await redis.set(
                    key,
                    orjson.dumps(grouped_klines).decode("utf-8"),
                    px=parsed_interval.to_milliseconds(),
                )
        except Exception as e:
            logger.error(
                f"Failed to save ticker klines to Redis (channel={channel}): {e}"
            )

    async def get_last_closed_klines(self, channel: Channel) -> list[dict[str, Any]]:
        """
        Get last closed klines for all intervals from Redis.

        Returns:
            List of kline data from all intervals
        """
        redis = self.redis
        if redis is None:
            logger.error("Storage: Redis connection is not initialized")
            return []

        all_klines: list[dict[str, Any]] = []
        try:
            for interval in settings.kline_intervals:
                key = f"{REDIS_LAST_CLOSED}:{channel}:{interval}"
                data = await redis.get(key)
                if data:
                    try:
                        klines = orjson.loads(data)
                        all_klines.extend(klines)
                    except Exception as e:
                        logger.warning(f"Failed to parse klines data for {key}: {e}")
        except Exception as e:
            logger.error(f"Failed to get last closed klines from Redis: {e}")

        return all_klines

    async def get_last_closed_ticker_klines(
        self, channel: Channel
    ) -> list[dict[str, Any]]:
        """
        Get last closed ticker klines for all intervals from Redis.

        Returns:
            List of ticker kline data from all intervals
        """
        redis = self.redis
        if redis is None:
            logger.error("Storage: Redis connection is not initialized")
            return []

        all_klines: list[dict[str, Any]] = []
        try:
            for interval in settings.kline_intervals:
                key = f"{REDIS_LAST_CLOSED_TICKERS}:{channel}:{interval}"
                data = await redis.get(key)
                if data:
                    try:
                        klines = orjson.loads(data)
                        all_klines.extend(klines)
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse ticker klines data for {key}: {e}"
                        )
        except Exception as e:
            logger.error(f"Failed to get last closed ticker klines from Redis: {e}")

        return all_klines

    async def close(self) -> None:
        """Close Redis connection if open."""
        if self.redis:
            try:
                await self.redis.aclose()
                logger.info("Storage Redis connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
            finally:
                self.redis = None
                self._is_connected = False
