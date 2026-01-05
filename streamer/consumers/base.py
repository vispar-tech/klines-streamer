"""Base consumer interface for pluggable outputs."""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseConsumer(ABC):
    """Abstract base class for all consumers."""

    def __init__(self, name: str) -> None:
        """
        Initialize the consumer.

        Args:
            name: Consumer name/identifier
        """
        self.name = name
        # Use logger like streamer.consumers.<consumer-name>
        self.logger = logging.getLogger(f"streamer.consumers.{name}")
        self._is_running = False

        # Validate consumer settings
        self.validate()

    @abstractmethod
    async def setup(self) -> None:
        """
        Set up the consumer (initialize connections, resources, etc.).

        This method should be called before start().
        """
        ...

    @abstractmethod
    async def start(self) -> None:
        """
        Start the consumer.

        This method should start any background tasks or connections.
        """
        ...

    @abstractmethod
    async def consume(self, data: List[Dict[str, Any]]) -> None:
        """
        Consume and process kline data.

        Args:
            data: Kline data dictionary containing symbol, interval, and kline data
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the consumer and cleanup resources.

        This method should gracefully shutdown the consumer.
        """
        ...

    @abstractmethod
    def validate(self) -> None:
        """
        Validate consumer-specific settings.

        This method should check that all required settings for this consumer
        are properly configured. Called during __init__.

        Raises:
            ValueError: If required settings are missing or invalid
        """
        ...

    def is_running(self) -> bool:
        """Check if the consumer is currently running."""
        return self._is_running

    async def __aenter__(self) -> "BaseConsumer":
        """Async context manager entry."""
        await self.setup()
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Async context manager exit."""
        await self.stop()
