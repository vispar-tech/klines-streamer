"""Main entry point for the Bybit Klines Streamer."""

import asyncio
import logging
import sys
from typing import List

from streamer.aggregator import Aggregator
from streamer.broadcaster import Broadcaster
from streamer.consumers import BaseConsumer, ConsumerManager
from streamer.settings import settings
from streamer.utils import setup_logging, validate_settings_symbols
from streamer.websocket import WebSocketClient

logger = logging.getLogger("streamer")


async def main_async() -> None:
    """Async main application entry point."""
    # Setup logging
    setup_logging()
    logger.info("Starting Bybit Klines Streamer")

    consumers: List[BaseConsumer] = []

    try:
        # Process and validate symbols
        await validate_settings_symbols()

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

            # Setup broadcaster
            broadcaster = Broadcaster(consumers)

            # Create aggregator that will send completed klines to broadcaster
            aggregator = Aggregator(broadcaster)

            # Create WebSocket client with pool support and connect to aggregator
            websocket_client = WebSocketClient(
                on_trade=aggregator.handle_trade, on_kline=aggregator.handle_kline
            )

            # Run aggregator timer
            await aggregator.start()

            await websocket_client.start()
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
