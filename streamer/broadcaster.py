"""
Broadcaster module for streaming data.

This module defines the Broadcaster class, which is responsible for
forwarding data to all registered consumer instances.
"""

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

from streamer.consumers.base import BaseConsumer
from streamer.types import Channel, DataType

logger = logging.getLogger(__name__)


class Broadcaster:
    """Broadcasts data to all registered consumers."""

    def __init__(self, consumers: Sequence[BaseConsumer]) -> None:
        """Initialize with a list of consumers."""
        self._consumers = consumers

    async def consume(
        self, channel: Channel, data_type: DataType, data: list[dict[str, Any]]
    ) -> None:
        """Send data to all consumers (in parallel), logging errors individually."""
        if data_type in ["klines", "tickers-klines"]:
            # Log only for closed klines, to avoid spam
            logger.info(
                f"Broadcasting {len(data)} items to "
                f"{len(self._consumers)} consumers in channel {channel}:{data_type}"
            )
        tasks = [
            consumer.consume(channel, data_type, data) for consumer in self._consumers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for consumer, result in zip(self._consumers, results, strict=False):
            if isinstance(result, Exception):
                consumer.logger.error(f"Consumer '{consumer.name}' failed: {result}")
