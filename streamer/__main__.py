"""Main entry point for the Bybit Klines Streamer."""

import asyncio
import logging
import sys
from typing import List

from streamer.consumers import BaseConsumer, ConsumerManager
from streamer.logging import setup_logging
from streamer.settings import settings

logger = logging.getLogger("streamer")


async def run_streamer(consumers: List[BaseConsumer]) -> None:
    """
    Run the main streamer logic.

    Args:
        consumers: List of active consumer instances
    """
    logger.info("Streamer is running...")

    # For now, just keep the consumers running
    # In the future, this will contain the main streaming logic
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Streamer received shutdown signal")


async def main_async() -> None:
    """Async main application entry point."""
    # Setup logging
    setup_logging()
    logger.info("Starting Bybit Klines Streamer")

    consumers: List[BaseConsumer] = []

    try:
        # Process and validate symbols
        await settings.process_symbols()

        # Validate configuration
        logger.info(f"Configured symbols: {settings.bybit_symbols}")
        logger.info(f"Configured intervals: {list(settings.kline_intervals)}")
        logger.info(f"Enabled consumers: {settings.enabled_consumers}")

        # Setup consumers
        consumers = await ConsumerManager.setup_consumers(settings.enabled_consumers)

        if consumers:
            # Initialize consumers
            await ConsumerManager.initialize_consumers(consumers)

            # Start consumers
            await ConsumerManager.start_consumers(consumers)

            # Run main streamer loop
            await run_streamer(consumers)
        else:
            logger.warning("No consumers to run, exiting...")

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)
    finally:
        # Shutdown consumers
        if consumers:
            await ConsumerManager.shutdown_consumers(consumers)
        logger.info("Streamer shutdown complete")


def main() -> None:
    """Start the main application."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Application interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
