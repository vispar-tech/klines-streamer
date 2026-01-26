"""Message routing between proxy components."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from proxy.websocket_server import WebSocketServer


class MessageRouter:
    """
    Routes messages between proxy components.

    Provides clean interface for broadcasting messages to all clients
    and sending messages to specific clients.
    """

    def __init__(self, websocket_server: "WebSocketServer") -> None:
        """
        Initialize message router.

        Args:
            websocket_server: WebSocket server instance for client communication
        """
        self._websocket_server = websocket_server

    async def broadcast(self, message: dict[str, Any] | bytes) -> None:
        """
        Broadcast message to all connected clients.

        Args:
            message: Message to broadcast (dict or compressed bytes)
        """
        await self._websocket_server.broadcast(message)

    async def send_to_client(self, client_id: str, message: dict[str, Any]) -> bool:
        """
        Send message to specific client.

        Args:
            client_id: Unique client identifier
            message: Message to send

        Returns:
            True if message was sent successfully, False otherwise
        """
        return await self._websocket_server.send_to_client(client_id, message)
