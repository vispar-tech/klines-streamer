"""Base WebSocket client with socket pool support."""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from streamer.settings import settings
from streamer.types import Channel
from streamer.utils.symbols import load_all_symbols

logger = logging.getLogger(__name__)


class WebSocketClient(ABC):
    """Base WebSocket client with support for socket pools."""

    def __init__(
        self,
        channel: Channel,
        on_trade: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_ticker: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
        on_symbols_count_changed: Callable[[int], None] | None = None,
    ) -> None:
        """Initialize WebSocket client with pool support.

        Args:
            channel: WebSocket channel to connect to
            on_trade: Callback function to handle incoming trade data
            on_ticker: Callback function to handle incoming ticker data
            on_symbols_count_changed: Optional callback with new total symbols count
        """
        self.symbols = settings.exchange_symbols
        self.on_trade = on_trade
        self.on_ticker = on_ticker
        self._on_symbols_count_changed = on_symbols_count_changed

        self.url = self._get_websocket_url(channel)
        self.pool_size = settings.exchange_socket_pool_size

        self.channel = channel  # store channel for logs

        # Pool management
        self._running = False
        self._socket_tasks: list[asyncio.Task[None]] = []
        self._sockets: dict[int, ClientConnection] = {}
        self._subscribed_symbols: dict[int, set[str]] = {}
        self._symbols_refresh_task: asyncio.Task[None] | None = None

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

        # Store references for periodic refresh (same sets as distributed_symbols)
        for i, socket_symbols in enumerate(distributed_symbols):
            self._subscribed_symbols[i] = socket_symbols

        # Create and start all sockets concurrently
        socket_tasks: list[asyncio.Task[None]] = []
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

        self._socket_tasks = socket_tasks
        self._running = True

        # Start periodic symbols refresh (every 15 min by default)
        refresh_interval = settings.exchange_symbols_refresh_interval_sec
        if refresh_interval > 0:
            self._symbols_refresh_task = asyncio.create_task(
                self._refresh_symbols_periodically(refresh_interval)
            )
            logger.info(
                f"Symbols refresh enabled every {refresh_interval}s on channel "
                f"{self.channel}"
            )

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

        # Cancel symbols refresh task
        if (
            self._symbols_refresh_task is not None
            and not self._symbols_refresh_task.done()
        ):
            self._symbols_refresh_task.cancel()
            try:
                await asyncio.wait_for(self._symbols_refresh_task, timeout=3)
            except TimeoutError:
                logger.warning(
                    "Timeout waiting for symbols refresh task to cancel "
                    f"on channel {self.channel}"
                )
            except asyncio.CancelledError:
                pass
            self._symbols_refresh_task = None

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
        self._subscribed_symbols.clear()

        logger.info(f"WebSocketClient stopped on channel {self.channel}")

    async def _refresh_symbols_periodically(self, interval_sec: int) -> None:
        """Refresh symbols list from exchange API and add new ones to subscriptions."""
        try:
            while True:
                await asyncio.sleep(interval_sec)
                if not self._running:
                    break
                logger.debug(
                    "Refreshing symbols list for %s on channel %s",
                    settings.exchange,
                    self.channel,
                )
                try:
                    new_symbols = await load_all_symbols(
                        settings.exchange_symbols_limit
                    )
                except Exception as e:
                    logger.error(
                        "Failed to load symbols for refresh on channel %s: %s",
                        self.channel,
                        e,
                    )
                    continue

                current_symbols: set[str] = set()
                for sym_set in self._subscribed_symbols.values():
                    current_symbols.update(sym_set)
                to_add = new_symbols - current_symbols
                if to_add:
                    logger.info(
                        "Found %s new symbols to subscribe on channel %s: %s",
                        len(to_add),
                        self.channel,
                        ", ".join(sorted(to_add)),
                    )
                    await self._add_new_symbols(list(to_add))
                    # Update settings so other components stay in sync
                    settings.exchange_symbols.update(to_add)
                    if self._on_symbols_count_changed is not None:
                        self._on_symbols_count_changed(len(settings.exchange_symbols))
        except asyncio.CancelledError:
            logger.debug("Symbols refresh task cancelled on channel %s", self.channel)
        except Exception as e:
            logger.error("Error in symbols refresh on channel %s: %s", self.channel, e)

    async def _add_new_symbols(self, new_symbols: list[str]) -> None:
        """Add new symbols to pool and subscribe only to them on each socket."""
        if not new_symbols:
            return
        if not self._subscribed_symbols:
            return

        pool_sizes = {i: len(s) for i, s in self._subscribed_symbols.items()}
        new_symbol_map: dict[int, list[str]] = {i: [] for i in self._subscribed_symbols}

        for symbol in new_symbols:
            idx = min(pool_sizes, key=lambda k: pool_sizes[k])
            self._subscribed_symbols[idx].add(symbol)
            pool_sizes[idx] += 1
            new_symbol_map[idx].append(symbol)

        for socket_id in self._subscribed_symbols:
            ws = self._sockets.get(socket_id)
            symbols_to_sub = new_symbol_map[socket_id]
            if ws and symbols_to_sub:
                try:
                    await self._subscribe(ws, set(symbols_to_sub))
                    logger.info(
                        "Subscribed to %s new symbol(s) on socket %s on channel %s: %s",
                        len(symbols_to_sub),
                        socket_id,
                        self.channel,
                        ", ".join(sorted(symbols_to_sub)[-5:]),
                    )
                except Exception as e:
                    logger.error(
                        "Error subscribing new symbols on socket %s on channel %s: %s",
                        socket_id,
                        self.channel,
                        e,
                    )

    def _distribute_symbols(self, symbols: set[str]) -> list[set[str]]:
        """Distribute symbols across socket pool."""
        if self.pool_size <= 1:
            return [set(symbols)]

        # Simple round-robin distribution
        distributed: list[set[str]] = [set() for _ in range(self.pool_size)]
        for i, symbol in enumerate(symbols):
            distributed[i % self.pool_size].add(symbol)

        return list(filter(lambda symbols: len(symbols), distributed))

    async def _run_socket(self, socket_id: int, symbols: set[str]) -> None:
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
                    logger.exception(
                        f"Error on channel {self.channel} (socket {socket_id}): "
                        f"{e}, reconnecting in 5 seconds..."
                    )
                    await asyncio.sleep(5)

        logger.info(f"Socket {socket_id} stopped on channel {self.channel}")

    @abstractmethod
    async def _subscribe(self, websocket: ClientConnection, symbols: set[str]) -> None:
        """Subscribe to streams for assigned symbols."""

    @abstractmethod
    async def _ping_loop(self, socket_id: int, websocket: ClientConnection) -> None:
        """Send periodic ping messages."""

    @abstractmethod
    async def _handle_message(
        self, message: websockets.Data, websocket: ClientConnection
    ) -> None:
        """Handle incoming WebSocket message."""
