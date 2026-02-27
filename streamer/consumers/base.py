"""Base consumer interface for pluggable outputs."""

import logging
from abc import ABC, abstractmethod
from typing import Any

from streamer.storage import Storage
from streamer.types import Channel, DataType


class BaseConsumer(ABC):
    """Define abstract base consumer class."""

    def __init__(self, storage: Storage, name: str) -> None:
        """Initialize consumer."""
        self.name = name
        self._storage = storage
        self.logger = logging.getLogger(f"streamer.consumers.{name}")
        self._is_running = False
        self.validate()

    @abstractmethod
    async def setup(self) -> None:
        """Set up consumer."""

    @abstractmethod
    async def start(self) -> None:
        """Start consumer."""

    @abstractmethod
    async def consume(
        self, channel: Channel, data_type: DataType, data: list[dict[str, Any]]
    ) -> None:
        """Consume and process data."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop consumer and clean up resources."""

    @abstractmethod
    def validate(self) -> None:
        """Validate consumer settings."""

    def is_running(self) -> bool:
        """Return running state."""
        return self._is_running

    async def __aenter__(self) -> "BaseConsumer":
        """Enter async context."""
        await self.setup()
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit async context."""
        await self.stop()
