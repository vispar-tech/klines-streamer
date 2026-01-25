"""Run the proxy application."""

import asyncio
import logging
import signal
import sys
from types import FrameType
from typing import Optional

from proxy.logging import setup_logging
from proxy.message_router import MessageRouter
from proxy.settings import settings
from proxy.upstream import UpstreamManager
from proxy.websocket_server import WebSocketServer

logger = logging.getLogger("proxy")


def setup_signal_handlers(stop_event: asyncio.Event) -> None:
    """Register signal handlers to set the stop event on termination signals."""

    def signal_handler(signum: int, frame: Optional[FrameType]) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def main() -> None:
    """Start the proxy application."""
    setup_logging()
    logger.info("Starting Klines Streamer Proxy")

    # todo: rework passing on client connected
    # Will be used by WebSocket server to trigger upstream initial data requests
    async def on_client_connected(client_id: str) -> None:
        try:
            await upstream_manager.request_initial_data(client_id)
            logger.info(f"Requested initial data for client {client_id}")
        except Exception as e:
            logger.error(f"Error requesting initial data for client {client_id}: {e}")

    # Create major components
    websocket_server = WebSocketServer(on_client_connected)
    message_router = MessageRouter(websocket_server)
    upstream_manager = UpstreamManager(message_router)

    stop_event = asyncio.Event()

    try:
        # Order: start WebSocket server first, then upstream connections
        await websocket_server.start()
        logger.info(
            "WebSocket server started on "
            f"{settings.websocket_host}:{settings.websocket_port}"
        )

        await upstream_manager.start()
        logger.info(f"Connected to upstream servers: {settings.upstream_connections}")

        logger.info("Proxy started successfully")

        # Setup signal handling after startup for clean shutdown
        setup_signal_handlers(stop_event)
        await stop_event.wait()

    except Exception as e:
        logger.error(f"Error starting proxy: {e}")
        raise
    finally:
        logger.info("Shutting down proxy...")

        # Stop both subsystems in logical order
        await websocket_server.stop()
        await upstream_manager.stop()

        logger.info("Proxy shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        sys.exit(1)
