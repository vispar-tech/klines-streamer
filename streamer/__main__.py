"""Main entry point for the Bybit Klines Streamer."""

import asyncio
import logging
import sys
from typing import List, Optional

import uvloop

from streamer.aggregator import Aggregator
from streamer.broadcaster import Broadcaster
from streamer.consumers import BaseConsumer, ConsumerManager
from streamer.settings import settings
from streamer.storage import Storage
from streamer.utils import setup_logging, validate_settings_symbols
from streamer.websockets import WebSocketClient, get_websocket_client

logger = logging.getLogger("streamer")


async def _validate_startup() -> tuple[Storage, List[BaseConsumer]]:
    # Create main storage processor that will storage all data from websocket
    storage = Storage()
    await storage.setup()
    # Process and validate symbols
    await validate_settings_symbols()

    # Validate configuration
    logger.info(f"Configured symbols: {settings.exchange_symbols}")
    logger.info(f"Configured intervals: {list(map(str, settings.kline_intervals))}")
    logger.info(f"Enabled consumers: {settings.enabled_consumers}")

    # Setup consumers
    consumers = await ConsumerManager.setup_consumers(
        storage, settings.enabled_consumers
    )
    return storage, consumers


async def main_async() -> None:
    """Async main application entry point."""
    # Setup logging
    setup_logging()
    logger.info("Starting Bybit Klines Streamer")

    consumers: List[BaseConsumer] = []
    websocket_client: Optional[WebSocketClient] = None
    spot_websocket_client: Optional[WebSocketClient] = None

    try:
        storage, consumers = await _validate_startup()

        if consumers:
            # Initialize consumers
            await ConsumerManager.initialize_consumers(consumers)

            # Start consumers
            await ConsumerManager.start_consumers(consumers)

            # Setup broadcaster
            broadcaster = Broadcaster(consumers)

            # Create aggregator that will send completed klines to broadcaster
            # We need separate aggregators to avoid bottleneck
            aggregator = Aggregator(broadcaster, storage, channel="linear")

            # Create WebSocket client with pool support and connect to aggregator
            websocket_client = get_websocket_client(
                channel="linear",
                on_trade=aggregator.handle_trade,
                on_ticker=aggregator.handle_ticker,
                on_symbols_count_changed=aggregator.set_total_symbols_count,
            )

            if settings.enable_spot_stream:
                if settings.exchange == "bingx":
                    logger.error("Spot streaming is not supported for BingX exchange.")
                    sys.exit(1)
                if settings.exchange == "bitget":
                    logger.error("Spot streaming is not supported for Bitget exchange.")
                    sys.exit(1)
                # The same for spot data
                spot_aggregator = Aggregator(broadcaster, storage, channel="spot")

                spot_websocket_client = get_websocket_client(
                    channel="spot",
                    on_trade=spot_aggregator.handle_trade,
                    on_ticker=spot_aggregator.handle_ticker,
                    on_symbols_count_changed=spot_aggregator.set_total_symbols_count,
                )

                # Run aggregator timer
                await asyncio.gather(
                    aggregator.start(),
                    spot_aggregator.start(),
                )

                await asyncio.gather(
                    websocket_client.start(),
                    spot_websocket_client.start(),
                )
            else:
                # Only run linear (no spot)
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
        # Stop WebSocket clients
        if websocket_client is not None:
            await websocket_client.stop()
        if spot_websocket_client is not None:
            await spot_websocket_client.stop()

        # Shutdown consumers
        if consumers:
            await ConsumerManager.shutdown_consumers(consumers)

        logger.info("Streamer shutdown complete")


def main() -> None:
    """Start the main application."""
    try:
        uvloop.run(main_async())
    except KeyboardInterrupt:
        logger.info("Application interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
