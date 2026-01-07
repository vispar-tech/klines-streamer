"""Pluggable consumers for outputting kline data."""

import logging
from typing import Dict, List, Set, Type

from streamer.consumers.base import BaseConsumer
from streamer.consumers.console import ConsoleConsumer
from streamer.consumers.redis import RedisConsumer
from streamer.consumers.websocket import WebSocketConsumer

logger = logging.getLogger(__name__)


class ConsumerRegistry:
    """Registry for managing consumer classes and instances."""

    def __init__(self) -> None:
        """Initialize the consumer registry."""
        self._registry: Dict[str, Type[BaseConsumer]] = {}

        # Register built-in consumers
        self.register("console", ConsoleConsumer)
        self.register("redis", RedisConsumer)
        self.register("websocket", WebSocketConsumer)

    def register(self, name: str, consumer_class: Type[BaseConsumer]) -> None:
        """
        Register a consumer class.

        Args:
            name: Unique name for the consumer
            consumer_class: Consumer class to register

        Raises:
            ValueError: If name is already registered or class is invalid
        """
        if name in self._registry:
            raise ValueError(f"Consumer '{name}' is already registered")

        # Type annotation already ensures consumer_class is a subclass of BaseConsumer

        self._registry[name] = consumer_class

    def unregister(self, name: str) -> None:
        """
        Unregister a consumer class.

        Args:
            name: Name of the consumer to unregister

        Raises:
            ValueError: If consumer is not registered
        """
        if name not in self._registry:
            raise ValueError(f"Consumer '{name}' is not registered")

        del self._registry[name]

    def get_consumer_class(self, name: str) -> Type[BaseConsumer]:
        """
        Get a consumer class by name.

        Args:
            name: Name of the consumer

        Returns:
            Consumer class

        Raises:
            ValueError: If consumer is not registered
        """
        if name not in self._registry:
            raise ValueError(f"Unknown consumer: {name}")

        return self._registry[name]

    def list_consumers(self) -> List[str]:
        """List all registered consumer names."""
        return list(self._registry.keys())

    def validate_consumers(self, consumer_names: Set[str]) -> List[str]:
        """
        Validate a list of consumer names.

        Args:
            consumer_names: List of consumer names to validate

        Returns:
            List of validation errors (empty if all valid)

        Raises:
            ValueError: If any consumer names are invalid
        """
        errors: list[str] = []
        for name in consumer_names:
            if name not in self._registry:
                errors.append(f"Unknown consumer: {name}")

        if errors:
            raise ValueError("; ".join(errors))

        return errors

    def create_consumers(self, consumer_names: Set[str]) -> List[BaseConsumer]:
        """
        Create consumer instances for the given names.

        Args:
            consumer_names: List of consumer names to create

        Returns:
            List of consumer instances

        Raises:
            ValueError: If any consumer names are invalid
        """
        self.validate_consumers(consumer_names)

        consumers: list[BaseConsumer] = []
        for name in consumer_names:
            consumer_class = self._registry[name]
            consumer = consumer_class(name)
            consumers.append(consumer)

        return consumers

    def create_consumer(self, name: str) -> BaseConsumer:
        """
        Create a single consumer instance.

        Args:
            name: Name of the consumer to create

        Returns:
            Consumer instance

        Raises:
            ValueError: If consumer name is invalid
        """
        consumer_class = self.get_consumer_class(name)
        return consumer_class(name)


class ConsumerManager:
    """Manages consumer lifecycle and operations."""

    _registry = ConsumerRegistry()
    _logger = logging.getLogger(__name__)

    @classmethod
    def register_consumer(cls, name: str, consumer_class: Type[BaseConsumer]) -> None:
        """
        Register a custom consumer class.

        Args:
            name: Unique name for the consumer
            consumer_class: Consumer class to register
        """
        cls._registry.register(name, consumer_class)

    @classmethod
    def list_available_consumers(cls) -> List[str]:
        """List all available consumer names."""
        return cls._registry.list_consumers()

    @classmethod
    async def setup_consumers(cls, enabled_consumers: Set[str]) -> List[BaseConsumer]:
        """
        Set up consumers.

        Args:
            enabled_consumers: List of consumer names to enable

        Returns:
            List of initialized consumer instances

        Raises:
            ValueError: If consumer creation fails or settings are invalid
        """
        cls._logger.info("Setting up consumers...")

        # Log available consumers
        available = cls.list_available_consumers()
        cls._logger.info(f"Available consumers: {available}")

        if not enabled_consumers:
            cls._logger.warning("No consumers enabled in configuration")
            return []

        try:
            # Create consumers (validation happens in __init__)
            consumers = cls._registry.create_consumers(enabled_consumers)
            cls._logger.info(
                f"Created {len(consumers)} consumer(s): {enabled_consumers}"
            )
            return consumers
        except ValueError as e:
            cls._logger.error(f"Consumer setup failed: {e}")
            cls._logger.error(f"Available consumers: {available}")
            raise

    @classmethod
    async def initialize_consumers(cls, consumers: List[BaseConsumer]) -> None:
        """
        Initialize all consumers (setup phase).

        Args:
            consumers: List of consumer instances to initialize
        """
        cls._logger.info("Initializing consumers...")

        for consumer in consumers:
            try:
                cls._logger.info(f"Setting up consumer: {consumer.name}")
                await consumer.setup()
            except Exception as e:
                cls._logger.error(f"Failed to setup consumer {consumer.name}: {e}")
                raise

    @classmethod
    async def start_consumers(cls, consumers: List[BaseConsumer]) -> None:
        """
        Start all consumers.

        Args:
            consumers: List of consumer instances to start
        """
        cls._logger.info("Starting consumers...")

        for consumer in consumers:
            try:
                cls._logger.info(f"Starting consumer: {consumer.name}")
                await consumer.start()
            except Exception as e:
                cls._logger.error(f"Failed to start consumer {consumer.name}: {e}")
                raise

    @classmethod
    async def shutdown_consumers(cls, consumers: List[BaseConsumer]) -> None:
        """
        Shutdown all consumers gracefully.

        Args:
            consumers: List of consumer instances to shutdown
        """
        cls._logger.info("Shutting down consumers...")

        # Stop consumers in reverse order
        for consumer in reversed(consumers):
            try:
                cls._logger.info(f"Stopping consumer: {consumer.name}")
                await consumer.stop()
            except Exception as e:
                cls._logger.error(f"Error stopping consumer {consumer.name}: {e}")


__all__ = [
    "BaseConsumer",
    "ConsoleConsumer",
    "ConsumerManager",
    "ConsumerRegistry",
    "RedisConsumer",
    "WebSocketConsumer",
]
