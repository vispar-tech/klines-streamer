"""
Example of how to create and register a custom consumer.

This example demonstrates how users can extend the streamer with their own
custom consumers for different output destinations.
"""

import asyncio
import sys
from pathlib import Path
from typing import Any, Dict

# Add parent path to sys.path for local development/example usage
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import orjson

from streamer.consumers import ConsumerManager
from streamer.consumers.base import BaseConsumer


class FileConsumer(BaseConsumer):
    """Example consumer that writes kline data to an output path."""

    def __init__(self, output_path: str | None = None) -> None:
        """
        Initialize file consumer with configurable output path.

        If no output_path is provided, defaults to './output/kline_data.jsonl'.
        """
        super().__init__("file")

        # Ensure we always write into './output' directory
        output_dir = Path("./output")
        output_dir.mkdir(parents=True, exist_ok=True)
        if output_path is None:
            resolved_output_path = output_dir / "kline_data.jsonl"
        else:
            resolved_output_path = output_dir / output_path
        self.output_path = str(resolved_output_path)

    def validate(self) -> None:
        """Validate consumer."""

    async def setup(self) -> None:
        """Set up file consumer."""
        self.logger.info(f"Setting up file consumer, will write to: {self.output_path}")

    async def start(self) -> None:
        """Start file consumer."""
        self.logger.info("Starting file consumer")
        self._is_running = True

    async def consume(self, data: Dict[str, Any]) -> None:
        """
        Consume kline data and write to output path.

        Args:
            data: Kline data dictionary
        """
        if not self._is_running:
            return

        try:
            # Write as JSON Lines format using context manager
            output_dir = Path(self.output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            line = orjson.dumps(data).decode("utf-8") + "\n"
            with Path(self.output_path).open("a", encoding="utf-8") as f:
                f.write(line)
                f.flush()  # Ensure data is written immediately

            self.logger.debug(f"Wrote kline data to output: {self.output_path}")

        except Exception as e:
            self.logger.error(f"Failed to write kline data to output path: {e}")

    async def stop(self) -> None:
        """Stop file consumer (nothing to cleanup since files not held open)."""
        self.logger.info("Stopping file consumer")
        self._is_running = False


# Register custom consumer
ConsumerManager.register_consumer("file", FileConsumer)


if __name__ == "__main__":
    # Example usage
    print("Custom consumer registered!")
    print("You can now use 'file' in your ENABLED_CONSUMERS setting")
    print("Built-in consumers available: 'console', 'redis', 'websocket'")

    # You can also create and use them directly
    async def demo() -> None:
        """Demonstrate direct consumer usage."""
        output_path = "custom_output.jsonl"
        consumer = FileConsumer(output_path=output_path)
        await consumer.setup()
        await consumer.start()

        # Simulate some kline data
        test_data = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "timestamp": 1234567890,
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.5,
        }

        await consumer.consume(test_data)
        await consumer.stop()

        print(f"Saved test kline data to: ./output/{output_path}")

    print("Now we have this consumers: ", ConsumerManager.list_available_consumers())

    asyncio.run(demo())
