"""Base WebSocket client with socket pool support."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, Dict, List, Set

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from streamer.settings import settings
from streamer.types import Channel

logger = logging.getLogger(__name__)


class WebSocketClient(ABC):
    """Base WebSocket client with support for socket pools."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
    ) -> None:
        """Initialize WebSocket client with pool support.

        Args:
            channel: WebSocket channel to connect to
            on_trade: Callback function to handle incoming trade data
            on_ticker: Callback function to handle incoming ticker data
        """
        self.symbols = settings.exchange_symbols
        self.on_trade = on_trade
        self.on_ticker = on_ticker

        self.url = self._get_websocket_url(channel)
        self.pool_size = settings.exchange_socket_pool_size

        self.channel = channel  # store channel for logs

        # Pool management
        self._running = False
        self._socket_tasks: List[asyncio.Task[None]] = []
        self._sockets: Dict[int, ClientConnection] = {}

    @abstractmethod
    def _get_websocket_url(self, channel: Channel) -> str:
        """Return the WebSocket URL for the specific exchange."""
        ...

    async def start(self) -> None:
        """Start the socket pool processor."""
        if self._running:
            logger.warning(f"WebSocketClient already running on channel {self.channel}")
            return

        logger.info(
            f"Starting WebSocketClient with pool size {self.pool_size} "
            f"on channel {self.channel}"
        )

        # Distribute symbols across sockets
        distributed_symbols = self._distribute_symbols(self.symbols)
        total_pool_size = len(distributed_symbols)
        if total_pool_size != self.pool_size:
            logger.info(
                f"Adjust pool size {self.pool_size} -> {total_pool_size} "
                f"on channel {self.channel}"
            )

        # Create and start all sockets concurrently
        socket_tasks: List[asyncio.Task[None]] = []
        for i, socket_symbols in enumerate(distributed_symbols):
            if len(socket_symbols) == 0:
                continue

            logger.info(
                f"Creating socket {i + 1}/{total_pool_size} "
                f"with {len(socket_symbols)} symbols on channel {self.channel}"
            )

            # Start socket as background task
            task = asyncio.create_task(self._run_socket(i, socket_symbols))
            socket_tasks.append(task)

        self._running = True
        # Wait for all sockets to initialize
        await asyncio.gather(*socket_tasks, return_exceptions=True)

        logger.info(
            f"WebSocketClient started with {len(self._sockets)} sockets "
            f"on channel {self.channel}"
        )

    async def stop(self) -> None:
        """Stop all WebSocket connections."""
        logger.info(f"Stopping WebSocketClient on channel {self.channel}...")
        self._running = False

        # Cancel all socket tasks
        for task in self._socket_tasks:
            task.cancel()

        # Close all connections
        close_tasks: list[Coroutine[Any, Any, None]] = []
        for socket in self._sockets.values():
            close_tasks.append(socket.close())

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        self._sockets.clear()
        self._socket_tasks.clear()

        logger.info(f"WebSocketClient stopped on channel {self.channel}")

    def _distribute_symbols(self, symbols: set[str]) -> List[set[str]]:
        """Distribute symbols across socket pool."""
        if self.pool_size <= 1:
            return [set(symbols)]

        # Simple round-robin distribution
        distributed: list[set[str]] = [set() for _ in range(self.pool_size)]
        for i, symbol in enumerate(symbols):
            distributed[i % self.pool_size].add(symbol)

        return list(filter(lambda symbols: len(symbols), distributed))

    async def _run_socket(self, socket_id: int, symbols: Set[str]) -> None:
        """Run a single WebSocket connection."""
        while self._running:
            try:
                logger.info(
                    f"Connecting to {self.url} on channel {self.channel} "
                    f"(socket {socket_id})"
                )
                async with websockets.connect(self.url) as websocket:
                    self._sockets[socket_id] = websocket
                    logger.info(
                        f"Connected on channel {self.channel} (socket {socket_id})"
                    )

                    # Subscribe to assigned symbols
                    await self._subscribe(websocket, symbols)

                    # Start ping task
                    ping_task = asyncio.create_task(
                        self._ping_loop(socket_id, websocket)
                    )

                    try:
                        # Message handling loop
                        async for message in websocket:
                            await self._handle_message(message, websocket)
                    finally:
                        ping_task.cancel()
                        self._sockets.pop(socket_id, None)

            except ConnectionClosed:
                if self._running:
                    logger.warning(
                        f"Reconnecting in 5s on channel {self.channel} "
                        f"(socket {socket_id})..."
                    )
                    await asyncio.sleep(5)
            except Exception as e:
                if self._running:
                    logger.error(
                        f"Error on channel {self.channel} (socket {socket_id}): "
                        f"{e}, reconnecting in 5 seconds..."
                    )
                    await asyncio.sleep(5)

        logger.info(f"Socket {socket_id} stopped on channel {self.channel}")

    @abstractmethod
    async def _subscribe(self, websocket: ClientConnection, symbols: Set[str]) -> None:
        """Subscribe to streams for assigned symbols."""

    @abstractmethod
    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""

    @abstractmethod
    async def _handle_message(
        self, message: websockets.Data, websocket: ClientConnection
    ) -> None:
        """Handle incoming WebSocket message."""
