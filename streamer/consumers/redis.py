"""Redis consumer for publishing kline data."""

from typing import Any, Dict, List

import orjson
from redis.asyncio import Redis
from redis.asyncio import from_url as connection_from_url

from streamer.consumers.base import BaseConsumer
from streamer.settings import settings
from streamer.types import Channel, DataType


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
        if not settings.redis_main_key:
            raise ValueError(
                "REDIS_MAIN_KEY is required when Redis consumer is enabled"
            )

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

    async def consume(
        self, channel: Channel, data_type: DataType, data: List[Dict[str, Any]]
    ) -> None:
        """
        Consume kline data and publish to Redis.

        Args:
            data: Kline data dictionary containing symbol, interval, and kline data
        """
        if not self._is_running or not self.redis:
            return

        try:
            # Serialize data using orjson for better performance
            message = orjson.dumps(
                {"channel": channel, "data_type": data_type, "data": data}
            ).decode("utf-8")

            # Publish all kline data to the single configured main key
            if not settings.redis_main_key:
                self.logger.error("Redis main key is not configured")
                return

            await self.redis.publish(settings.redis_main_key, message)

            self.logger.debug(
                "Published kline data to Redis main key: "
                f"{settings.redis_main_key}, channel: {channel}"
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
