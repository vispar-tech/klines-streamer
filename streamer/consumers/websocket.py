"""WebSocket consumer for streaming kline data."""

import asyncio
import contextlib
from typing import Any, Dict, List, Set

import orjson
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from streamer.consumers.base import BaseConsumer
from streamer.settings import settings


class WebSocketConnectionManager:
    """Manages WebSocket client connections."""

    def __init__(self) -> None:
        """Initialize the WebSocketConnectionManager with an empty connection set."""
        self.connections: Set[ServerConnection] = set()

    def add(self, ws: ServerConnection) -> None:
        """
        Add a WebSocket connection to the managed set.

        Args:
            ws: The ServerConnection connection to add.
        """
        self.connections.add(ws)

    def remove(self, ws: ServerConnection) -> None:
        """
        Remove (discard) a WebSocket connection from the managed set.

        Args:
            ws: The ServerConnection connection to remove.
        """
        self.connections.discard(ws)

    async def broadcast(self, message: str) -> None:
        """
        Broadcast a message to all managed WebSocket connections.

        Args:
            message: The string message to send to all clients.
        """
        if not self.connections:
            return
        websockets = self.connections.copy()
        to_remove: set[ServerConnection] = set()
        for ws in websockets:
            try:
                await ws.send(message)
            except ConnectionClosed:
                to_remove.add(ws)
            except Exception:
                to_remove.add(ws)
        for ws in to_remove:
            self.connections.discard(ws)

    def count(self) -> int:
        """
        Return the number of currently managed WebSocket connections.

        Returns:
            int: The number of connections.
        """
        return len(self.connections)


class WebSocketConsumer(BaseConsumer):
    """WebSocket consumer for streaming kline data to connected clients."""

    def __init__(self, name: str = "websocket") -> None:
        """Initialize the WebSocketConsumer and its resource manager."""
        super().__init__(name)
        self.connection_manager = WebSocketConnectionManager()
        self.server: Server | None = None
        self._server_task: asyncio.Task[None] | None = None

    def validate(self) -> None:
        """
        Validate WebSocket consumer settings.

        Checks that all WebSocket settings are configured.
        """
        from streamer.settings import settings

        if not settings.websocket_host:
            raise ValueError(
                "WEBSOCKET_HOST is required when WebSocket consumer is enabled"
            )
        if not settings.websocket_port:
            raise ValueError(
                "WEBSOCKET_PORT is required when WebSocket consumer is enabled"
            )
        if not settings.websocket_url:
            raise ValueError(
                "WEBSOCKET_URL is required when WebSocket consumer is enabled"
            )
        if not settings.wss_auth_key:
            raise ValueError(
                "WSS_AUTH_KEY is required when WebSocket consumer is enabled"
            )
        if not settings.wss_auth_user:
            raise ValueError(
                "WSS_AUTH_USER is required when WebSocket consumer is enabled"
            )

    async def authenticate(self, ws: ServerConnection) -> bool:
        """
        Authenticate a WebSocket client.

        Expects the client to send authentication credentials as the first message
        in the format: {"user": "username", "key": "auth_key"}

        Args:
            ws: The ServerConnection representing the client connection.

        Returns:
            bool: True if the client is authenticated, False otherwise.
        """
        try:
            auth_message = await asyncio.wait_for(ws.recv(), timeout=10.0)
            try:
                auth_data = orjson.loads(auth_message)
                user = auth_data.get("user")
                key = auth_data.get("key")
            except Exception:
                await ws.send(orjson.dumps({"error": "Invalid auth format"}).decode())
                self.logger.warning("Invalid authentication message format")
                return False

            if (
                settings.wss_auth_user
                and settings.wss_auth_key
                and user == settings.wss_auth_user
                and key == settings.wss_auth_key
            ):
                await ws.send(orjson.dumps({"status": "authenticated"}).decode())
                self.logger.info(f"Client authenticated: {ws.remote_address}")
                return True
            await ws.send(orjson.dumps({"error": "Authentication failed"}).decode())
            self.logger.warning(f"Authentication failed for: {ws.remote_address}")
            return False
        except asyncio.TimeoutError:
            await ws.send(orjson.dumps({"error": "Authentication timeout"}).decode())
            self.logger.warning(f"Authentication timeout for: {ws.remote_address}")
            return False
        except ConnectionClosed:
            self.logger.info(f"Auth disconnect: {ws.remote_address}")
            return False

    async def _client_handler(self, ws: ServerConnection) -> None:
        """
        Handle a single WebSocket client connection.

        Handles authentication and ping/pong messages.
        Args:
            ws: The ServerConnection representing the client connection.
        """
        client_address = ws.remote_address
        self.logger.info(f"New client connection: {client_address}")

        try:
            if not await self.authenticate(ws):
                return

            self.connection_manager.add(ws)
            self.logger.info(
                f"Client authenticated. Total: {self.connection_manager.count()}"
            )

            async for message in ws:
                try:
                    data = orjson.loads(message)
                    if data.get("type") == "ping":
                        await ws.send(orjson.dumps({"type": "pong"}).decode())
                except Exception as err:
                    self.logger.error(
                        f"Error handling websocket message "
                        f"from {ws.remote_address}: {err}",
                        exc_info=True,
                    )

        except ConnectionClosed:
            self.logger.info(f"Client disconnected: {client_address}")
        except Exception as e:
            self.logger.error(f"Error handling client {client_address}: {e}")
        finally:
            self.connection_manager.remove(ws)
            self.logger.info(
                f"Connection cleaned up. Total: {self.connection_manager.count()}"
            )

    async def setup(self) -> None:
        """
        Set up resources for the WebSocket consumer.

        Note: Actual server instantiation occurs in start().
        """
        self.logger.info("Setting up WebSocket consumer")
        # Server will be started in start()

    async def start(self) -> None:
        """
        Start the WebSocket consumer and run the websocket server.

        Sets up the WebSocket server and task to keep it running.
        """
        self.logger.info("Starting WebSocket consumer")

        if not settings.websocket_host or not settings.websocket_port:
            raise ValueError("WebSocket host and port must be configured")

        host = settings.websocket_host
        port = settings.websocket_port

        async def handler(ws: ServerConnection) -> None:
            """WebSocket server handler wrapper for _client_handler."""
            await self._client_handler(ws)

        try:
            # Start the websockets server using async context
            self.server = await serve(
                handler, host, port, reuse_address=True, reuse_port=True
            )
            self._is_running = True
            self._server_task = asyncio.create_task(self.server.wait_closed())
            self.logger.info(f"WebSocket server started on {host}:{port}")
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket server: {e}")
            raise

    async def consume(self, data: List[Dict[str, Any]]) -> None:
        """
        Consume kline data and broadcast to WebSocket clients.

        Args:
            data: Kline data dictionary containing symbol, interval, and candle data.
        """
        if not self._is_running:
            return

        try:
            message = orjson.dumps(data).decode("utf-8")
            await self.connection_manager.broadcast(message)

            c = self.connection_manager.count()
            self.logger.debug(f"Broadcasted kline data to {c} WebSocket client(s)")
        except Exception as e:
            self.logger.error(f"Broadcast failed: {e}")

    async def stop(self) -> None:
        """
        Stop the WebSocket consumer and clean up resources.

        This involves closing all active client connections and
        shutting down the server.
        """
        self.logger.info("Stopping WebSocket consumer")
        self._is_running = False

        try:
            active_connections = self.connection_manager.count()
            if active_connections > 0:
                self.logger.info(f"Closing {active_connections} WebSocket connections")
                close_message = orjson.dumps({"type": "server_shutdown"}).decode()
                await self.connection_manager.broadcast(close_message)

            if self.server:
                self.server.close()
                await self.server.wait_closed()
                self.logger.info("WebSocket server closed")

            if self._server_task and not self._server_task.done():
                self._server_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._server_task

        except Exception as e:
            self.logger.error(f"Error during WebSocket consumer shutdown: {e}")
        finally:
            self.server = None
            self._server_task = None
