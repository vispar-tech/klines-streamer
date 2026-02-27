"""WebSocket server for downstream clients."""

import asyncio
import contextlib
import gzip
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import orjson
from websockets.asyncio.server import Server, ServerConnection, serve

from proxy.settings import settings

logger = logging.getLogger(__name__)


def gzip_compress(data: bytes) -> bytes:
    """Compress bytes data using gzip and return bytes."""
    return gzip.compress(data, compresslevel=5)


class WebSocketServer:
    """WebSocket server for downstream clients."""

    def __init__(self, on_client_connected: Callable[[str], Awaitable[None]]) -> None:
        """Initialize WebSocket server."""
        self.connections: set[ServerConnection] = set()
        self.server: Server | None = None
        self._running: bool = False
        self._on_client_connected = on_client_connected
        self._client_connections: dict[str, ServerConnection] = {}

    def add_client_connection(
        self, client_id: str, websocket: ServerConnection
    ) -> None:
        """Add client connection mapping."""
        self._client_connections[client_id] = websocket

    def remove_client_connection(self, client_id: str) -> None:
        """Remove client connection mapping."""
        self._client_connections.pop(client_id, None)

    def get_client_connection(self, client_id: str) -> ServerConnection | None:
        """Get client connection by ID."""
        return self._client_connections.get(client_id)

    async def send_to_client(self, client_id: str, message: dict[str, Any]) -> bool:
        """Send message to specific client."""
        websocket = self.get_client_connection(client_id)
        if websocket:
            try:
                await self.send_json(websocket, message)
                return True
            except Exception as e:
                logger.error(f"Failed to send message to client {client_id}: {e}")
                # Remove broken connection
                self.remove_client_connection(client_id)
        return False

    async def start(self) -> None:
        """Start WebSocket server."""
        if self._running:
            return

        self._running = True
        host = settings.websocket_host
        port = settings.websocket_port

        logger.info(f"Starting WebSocket server on {host}:{port}")

        async def handler(websocket: ServerConnection) -> None:
            await self._handle_client(websocket)

        self.server = await serve(
            handler, host, port, reuse_address=True, reuse_port=True
        )

        logger.info(f"WebSocket server started on {host}:{port}")

    async def stop(self) -> None:
        """Stop WebSocket server."""
        if not self._running:
            return

        self._running = False
        logger.info("Stopping WebSocket server")

        # Close all client connections
        close_tasks: list[asyncio.Task[None]] = []
        shutdown_message = orjson.dumps({"type": "server_shutdown"}).decode()
        for conn in list(self.connections):
            with contextlib.suppress(Exception):
                await conn.send(shutdown_message)
            close_tasks.append(asyncio.create_task(conn.close()))

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        self.connections.clear()

    async def send_gzip_json(self, ws: ServerConnection, data: dict[str, Any]) -> None:
        """Send JSON as gzipped binary data to the websocket."""
        try:
            payload = orjson.dumps(data)
            compressed = gzip_compress(payload)
            await ws.send(compressed)
        except Exception as e:
            logger.error(f"Failed to send gzip json to client: {e}")

    async def send_json(self, ws: ServerConnection, data: dict[str, Any]) -> None:
        """Send JSON as uncompressed text to the websocket."""
        try:
            payload = orjson.dumps(data).decode()
            await ws.send(payload)
        except Exception as e:
            logger.error(f"Failed to send json to client: {e}")

    async def broadcast(self, message: dict[str, Any] | bytes) -> None:
        """Broadcast message to all connected clients."""
        if not self.connections:
            return

        # If message is already compressed bytes (from upstream), send as-is
        if isinstance(message, bytes):
            compressed_message = message
        else:
            # If message is dict, compress it
            payload = orjson.dumps(message)
            compressed_message = gzip_compress(payload)

        websockets = set(self.connections)
        to_remove: set[ServerConnection] = set()

        for ws in websockets:
            try:
                await ws.send(compressed_message)
            except Exception:
                to_remove.add(ws)

        for ws in to_remove:
            self.connections.discard(ws)

        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} broken connections")

    async def _handle_client(self, websocket: ServerConnection) -> None:
        """Handle individual client connection."""
        client_address = websocket.remote_address
        client_id = str(uuid.uuid4())
        logger.info(f"New client connection: {client_address} (id: {client_id})")

        try:
            # Authenticate client
            authenticated = await self._authenticate_client(websocket)
            if not authenticated:
                return

            self.connections.add(websocket)
            self.add_client_connection(client_id, websocket)
            logger.info(f"Client authenticated. Total clients: {len(self.connections)}")

            # Send welcome message with available sources
            welcome_message = {
                "type": "welcome",
                "sources": settings.upstream_connections,
                "message": "Connected to unified kline stream",
            }
            await self.send_json(websocket, welcome_message)

            # Notify about new client connection for initial data request
            try:
                await self._on_client_connected(client_id)
            except Exception as e:
                logger.error(f"Error in client connected callback: {e}")

            # Keep connection alive
            async for message in websocket:
                try:
                    data = orjson.loads(message)
                    if data.get("type") == "ping":
                        await self.send_json(websocket, {"type": "pong"})
                except Exception as e:
                    logger.error(f"Error handling client message: {e}")

        except Exception as e:
            logger.error(f"Error handling client {client_address}: {e}")
        finally:
            self.connections.discard(websocket)
            self.remove_client_connection(client_id)
            logger.info(
                f"Client disconnected: {client_address} (id: {client_id}). "
                f"Total clients: {len(self.connections)}"
            )

    async def _authenticate_client(self, websocket: ServerConnection) -> bool:
        """Authenticate client connection."""
        try:
            # Wait for authentication message
            auth_message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            auth_data = orjson.loads(auth_message)

            user = auth_data.get("user")
            key = auth_data.get("key")

            if user == settings.wss_auth_user and key == settings.wss_auth_key:
                await self.send_json(websocket, {"status": "authenticated"})
                logger.info(f"Client authenticated: {websocket.remote_address}")
                return True
            await self.send_json(websocket, {"error": "Authentication failed"})
            logger.warning(f"Authentication failed for: {websocket.remote_address}")
            return False

        except TimeoutError:
            await self.send_json(websocket, {"error": "Authentication timeout"})
            logger.warning(f"Authentication timeout for: {websocket.remote_address}")
            return False
        except Exception as e:
            await self.send_json(websocket, {"error": "Invalid auth format"})
            logger.warning(f"Invalid authentication message: {e}")
            return False
