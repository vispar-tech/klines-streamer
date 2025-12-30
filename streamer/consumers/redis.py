"""Redis consumer for publishing kline data."""

from typing import Any, Dict, List

import orjson
from redis.asyncio import Redis
from redis.asyncio import from_url as connection_from_url

from streamer.consumers.base import BaseConsumer
from streamer.settings import settings


class RedisConsumer(BaseConsumer):
    """Redis consumer for publishing kline data to a Redis pubsub channel."""

    def __init__(self, name: str = "redis") -> None:
        """Initialize Redis consumer."""
        super().__init__(name)
        self.redis: "Redis | None" = None

    def validate(self) -> None:
        """
        Validate Redis consumer settings.

        Checks that Redis URL and channel are configured.
        """
        if not settings.redis_url:
            raise ValueError("REDIS_URL is required when Redis consumer is enabled")
        if not settings.redis_channel:
            raise ValueError("REDIS_CHANNEL is required when Redis consumer is enabled")

    async def setup(self) -> None:
        """Set up Redis connection and resources."""
        self.logger.info("Setting up Redis consumer")

        if not settings.redis_url:
            raise ValueError("Redis URL is not configured")

        try:
            self.redis = connection_from_url(  # type: ignore[no-untyped-call]
                settings.redis_url,
                decode_responses=True,
            )
            self.logger.info(f"Connected to Redis at {settings.redis_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def start(self) -> None:
        """Start the Redis consumer."""
        self.logger.info("Starting Redis consumer")
        self._is_running = True

    async def consume(self, data: List[Dict[str, Any]]) -> None:
        """
        Consume kline data and publish to Redis.

        Args:
            data: Kline data dictionary containing symbol, interval, and candle data
        """
        if not self._is_running or not self.redis:
            return

        try:
            # Serialize data using orjson for better performance
            message = orjson.dumps(data).decode("utf-8")

            # Publish all kline data to the single configured channel
            if not settings.redis_channel:
                self.logger.error("Redis channel is not configured")
                return

            await self.redis.publish(settings.redis_channel, message)

            self.logger.debug(
                f"Published kline data to Redis channel: {settings.redis_channel}"
            )

        except Exception as e:
            self.logger.error(f"Failed to publish kline data to Redis: {e}")
            # Don't re-raise to avoid breaking the stream

    async def stop(self) -> None:
        """Stop the Redis consumer and cleanup resources."""
        self.logger.info("Stopping Redis consumer")
        self._is_running = False

        if self.redis:
            try:
                await self.redis.aclose()
                self.logger.info("Redis connection closed")
            except Exception as e:
                self.logger.error(f"Error closing Redis connection: {e}")
            finally:
                self.redis = None
