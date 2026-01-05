"""Bybit WebSocket client with socket pool support."""

import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, List, Set

import orjson
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from streamer.settings import settings

logger = logging.getLogger(__name__)


class WebSocketClient:
    """Bybit WebSocket client with support for socket pools."""

    def __init__(
        self,
        on_trade: Callable[[Any], Coroutine[Any, Any, None]],
        on_kline: Callable[[Any], Coroutine[Any, Any, None]],
        url: str = "wss://stream.bybit.com/v5/public/linear",
    ) -> None:
        """Initialize WebSocket client with pool support.

        Args:
            symbols: List of trading symbols to subscribe to
            on_trade: Callback function to handle incoming trade data
            url: WebSocket URL for Bybit
            pool_size: Number of concurrent WebSocket connections to use
        """
        self.symbols = settings.bybit_symbols
        self.on_trade = on_trade
        self.on_kline = on_kline
        self.url = url
        self.pool_size = settings.bybit_socket_pool_size

        if settings.klines_mode:
            self.topic_part = "kline."
            self.topic = "kline.{interval}.{symbol}"
        else:
            self.topic_part = "publicTrade."
            self.topic = "publicTrade.{symbol}"

        # Pool management
        self._running = False
        self._socket_tasks: List[asyncio.Task[None]] = []
        self._sockets: Dict[int, ClientConnection] = {}

    async def start(self) -> None:
        """Start the socket pool processor."""
        if self._running:
            logger.warning("WebSocketClient already running")
            return

        logger.info(f"Starting WebSocketClient with pool size {self.pool_size}")

        # Distribute symbols across sockets
        distributed_symbols = self._distribute_symbols(self.symbols)

        # Create and start all sockets concurrently
        socket_tasks: List[asyncio.Task[None]] = []
        for i, socket_symbols in enumerate(distributed_symbols):
            logger.info(
                f"Creating socket {i + 1}/{self.pool_size} "
                f"with {len(socket_symbols)} symbols"
            )
            if len(socket_symbols) == 0:
                logger.warning(f"No symbols for socket {i + 1}/{self.pool_size}")
                continue

            # Start socket as background task
            task = asyncio.create_task(self._run_socket(i, socket_symbols))
            socket_tasks.append(task)

        self._running = True
        # Wait for all sockets to initialize
        await asyncio.gather(*socket_tasks, return_exceptions=True)

        logger.info(f"WebSocketClient started with {len(self._sockets)} sockets")

    async def stop(self) -> None:
        """Stop all WebSocket connections."""
        logger.info("Stopping WebSocketClient...")
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

        logger.info("WebSocketClient stopped")

    def _distribute_symbols(self, symbols: set[str]) -> List[set[str]]:
        """Distribute symbols across socket pool."""
        if self.pool_size <= 1:
            return [set(symbols)]

        # Simple round-robin distribution
        distributed: list[set[str]] = [set() for _ in range(self.pool_size)]
        for i, symbol in enumerate(symbols):
            distributed[i % self.pool_size].add(symbol)

        return distributed

    async def _run_socket(self, socket_id: int, symbols: Set[str]) -> None:
        """Run a single WebSocket connection."""
        while self._running:
            try:
                logger.info(f"Socket {socket_id}: Connecting to {self.url}")
                async with websockets.connect(self.url) as websocket:
                    self._sockets[socket_id] = websocket
                    logger.info(f"Socket {socket_id}: Connected")

                    # Subscribe to assigned symbols
                    await self._subscribe(websocket, symbols)

                    # Start ping task
                    ping_task = asyncio.create_task(
                        self._ping_loop(socket_id, websocket)
                    )

                    try:
                        # Message handling loop
                        async for message in websocket:
                            await self._handle_message(message)
                    finally:
                        ping_task.cancel()
                        self._sockets.pop(socket_id, None)

            except ConnectionClosed:
                if self._running:
                    logger.warning(f"Socket {socket_id}: Reconnecting in 5s...")
                    await asyncio.sleep(5)
            except Exception as e:
                if self._running:
                    logger.error(
                        f"Socket {socket_id}: Error {e}, reconnecting in 5 seconds..."
                    )
                    await asyncio.sleep(5)

        logger.info(f"Socket {socket_id}: Stopped")

    async def _subscribe(self, websocket: ClientConnection, symbols: Set[str]) -> None:
        """Subscribe to public trade streams for assigned symbols."""
        if settings.klines_mode:
            args = [
                self.topic.format(symbol=symbol, interval=interval.to_bybit())
                for interval in settings.kline_intervals
                for symbol in symbols
            ]
        else:
            args = [self.topic.format(symbol=symbol) for symbol in symbols]
        subscription_msg = {"op": "subscribe", "args": args}

        await websocket.send(orjson.dumps(subscription_msg).decode("utf-8"))
        logger.info(f"Subscribed to streams: {args}")

    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""
        while self._running:
            try:
                ping_msg = {"req_id": f"ping_{socket_id}", "op": "ping"}
                await websocket.send(orjson.dumps(ping_msg).decode("utf-8"))
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Socket {socket_id}: Ping error: {e}")
                break

    async def _handle_message(self, message: str | bytes) -> None:
        """Handle incoming WebSocket message."""
        try:
            if isinstance(message, bytes):
                data = orjson.loads(message)
            else:
                data = orjson.loads(message)

            # Skip ping responses and subscription confirmations
            if data.get("op") == "ping" and data.get("ret_msg") == "pong":
                return
            if data.get("op") == "subscribe":
                logger.debug(f"Subscription confirmed: {data}")
                return

            topic = data["topic"]
            if not topic.startswith(self.topic_part):
                return

            trades = data["data"]
            if not trades:
                return

            if settings.klines_mode:
                await self.on_kline(data)
            else:
                # Handle trade data
                await self.on_trade(data)

        except orjson.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}, message: {message!s}")
