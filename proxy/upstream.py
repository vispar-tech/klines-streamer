"""Upstream WebSocket connections manager."""

import asyncio
import gzip
import logging
from typing import TYPE_CHECKING, Any

import orjson
from websockets.asyncio.client import ClientConnection, connect

if TYPE_CHECKING:
    from proxy.message_router import MessageRouter

from proxy.settings import settings


def gzip_compress(data: bytes) -> bytes:
    """Compress bytes data using gzip."""
    return gzip.compress(data, compresslevel=5)


logger = logging.getLogger(__name__)


class UpstreamManager:
    """
    Manages connections to upstream WebSocket servers.

    Handles connection lifecycle, message routing, and request/response
    coordination for initial data requests.
    """

    def __init__(
        self,
        message_router: "MessageRouter",
    ) -> None:
        """
        Initialize upstream manager.

        Args:
            message_router: Message router for communication with clients
        """
        self.connections: set[ClientConnection] = set()
        self._running: bool = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._message_router = message_router

    async def start(self) -> None:
        """Start all upstream connections."""
        if self._running:
            logger.debug("Upstream manager already running")
            return

        self._running = True
        logger.info("Starting upstream connections")

        # Start connection tasks for each upstream server
        for upstream_url in settings.upstream_connections:
            task = asyncio.create_task(self._connect_to_upstream(upstream_url))
            self._tasks.add(task)

        logger.info(
            f"Started {len(settings.upstream_connections)} upstream connection tasks"
        )

    async def stop(self) -> None:
        """Stop all upstream connections and clean up resources."""
        if not self._running:
            logger.debug("Upstream manager not running")
            return

        self._running = False
        logger.info("Stopping upstream connections")

        # Cancel all connection tasks
        logger.debug(f"Cancelling {len(self._tasks)} connection tasks")
        for task in self._tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close all connections
        active_connections = len(self.connections)
        if active_connections > 0:
            logger.debug(f"Closing {active_connections} active connections")
            close_tasks = [conn.close() for conn in self.connections.copy()]
            await asyncio.gather(*close_tasks, return_exceptions=True)

        # Clean up state
        self.connections.clear()
        self._tasks.clear()

        logger.info("Upstream connections stopped and cleaned up")

    async def request_initial_data(self, client_id: str) -> None:
        """Request initial data from all upstream connections for specific client."""
        if not self.connections:
            logger.warning(f"No upstream connections available for client {client_id}")
            return

        request_message = {
            "type": "request_initial_data",
            "request_id": f"req_{client_id}",
        }
        request_json = orjson.dumps(request_message).decode()

        logger.info(
            f"Sending request_initial_data (client_id={client_id}) to "
            f"{len(self.connections)} upstream connections"
        )

        for conn in self.connections:
            try:
                await conn.send(request_json)
            except Exception as e:
                logger.error(
                    f"Failed to send request_initial_data (client_id={client_id}) "
                    f"to upstream: {e}"
                )

    async def _connect_to_upstream(self, upstream_url: str) -> None:
        """Connect to upstream WebSocket server."""
        while self._running:
            try:
                logger.info(f"Connecting to upstream: {upstream_url}")
                async with connect(upstream_url) as websocket:
                    self.connections.add(websocket)
                    logger.info(f"Connected to upstream: {upstream_url}")

                    await self._authenticate(websocket)
                    await self._process_messages(websocket, upstream_url)

            except Exception as e:
                logger.error(f"Upstream connection error for {upstream_url}: {e}")
                await asyncio.sleep(5)  # Reconnect delay

    async def _authenticate(self, websocket: ClientConnection) -> None:
        """Authenticate with upstream server."""
        try:
            auth_message = orjson.dumps(
                {"user": settings.wss_auth_user, "key": settings.wss_auth_key}
            ).decode()
            await websocket.send(auth_message)
            response = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            auth_response = orjson.loads(response)

            if auth_response.get("status") == "authenticated":
                logger.info("Successfully authenticated with upstream")
            else:
                logger.error(f"Authentication failed: {auth_response}")
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise

    async def _process_messages(
        self, websocket: ClientConnection, upstream_url: str
    ) -> None:
        """Process messages from upstream server."""
        try:
            async for message in websocket:
                await self._handle_message(message, upstream_url)
        except Exception as e:
            logger.error(f"Error in message processing for {upstream_url}: {e}")
        finally:
            self.connections.discard(websocket)

    async def _handle_message(self, message: str | bytes, upstream_url: str) -> None:
        """Handle individual message from upstream."""
        try:
            if isinstance(message, bytes):
                await self._handle_binary_message(message, upstream_url)
            else:
                await self._handle_json_message(message, upstream_url)
        except Exception as e:
            logger.error(f"Error processing message from {upstream_url}: {e}")

    async def _handle_binary_message(self, message: bytes, upstream_url: str) -> None:
        """Handle compressed binary kline data (no longer wraps with source)."""
        # Directly forward the compressed message as received, no source wrapping
        await self._message_router.broadcast(message)

    async def _handle_json_message(self, message: str, upstream_url: str) -> None:
        """Handle JSON text messages."""
        data = orjson.loads(message)

        # No longer add "source"

        # Check if this is a response to our request
        if self._is_request_response(data):
            await self._handle_request_response(data, upstream_url)
            return

        await self._message_router.broadcast(data)

    def _is_request_response(self, data: dict[str, Any]) -> bool:
        """Check if message is a response to our request."""
        request_id = data.get("request_id")
        if request_id is None:
            return False
        return isinstance(request_id, str) and request_id.startswith("req_")

    async def _handle_request_response(
        self, data: dict[str, Any], upstream_url: str
    ) -> None:
        """Forward a response from upstream to the correct client (by request_id)."""
        request_id = data.pop("request_id")
        if not request_id or not isinstance(request_id, str):
            logger.warning(
                "Request response missing or invalid "
                f"request_id from upstream: {upstream_url}"
            )
            return

        client_id = request_id.removeprefix("req_")

        logger.debug(
            f"Received response for client {client_id} from upstream: {upstream_url}"
        )

        if await self._message_router.send_to_client(client_id, data):
            logger.info(f"Forwarded initial data to client {client_id}")
        else:
            logger.warning(f"Could not forward initial data to client {client_id}")
