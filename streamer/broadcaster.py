"""
Broadcaster module for streaming data.

This module defines the Broadcaster class, which is responsible for
forwarding data to all registered consumer instances.
"""

import asyncio
import logging
from typing import Any, Dict, List, Sequence

from streamer.consumers.base import BaseConsumer

logger = logging.getLogger(__name__)


class Broadcaster:
    """Broadcasts data to all registered consumers."""

    def __init__(self, consumers: Sequence[BaseConsumer]) -> None:
        """Initialize with a list of consumers."""
        self._consumers = consumers

    async def handle(self, data: List[Dict[str, Any]]) -> None:
        """Send data to all consumers (in parallel), logging errors individually."""
        logger.info(
            f"Broadcasting {len(data)} items to {len(self._consumers)} consumers"
        )
        results = await asyncio.gather(
            *(consumer.consume(data) for consumer in self._consumers),
            return_exceptions=True,
        )
        for consumer, result in zip(self._consumers, results, strict=False):
            if isinstance(result, Exception):
                consumer.logger.error(f"Consumer '{consumer.name}' failed: {result}")
