"""Ultra-fast OHLC aggregator; timer and builder."""

import asyncio
import contextlib
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, cast

from streamer.broadcaster import Broadcaster
from streamer.settings import settings
from streamer.storage import Storage
from streamer.types import Channel
from streamer.utils.parse import parse_numbers

if TYPE_CHECKING:
    from streamer.types import Interval

logger = logging.getLogger(__name__)

KLINE_MAX_WAIT_TIME = 20.0
KLINE_SLEEP_TIME = 0.01


class Aggregator:
    """OHLC aggregator with interval synchronization."""

    __slots__ = (
        "_broadcaster",
        "_channel",
        "_interval_ms_map",
        "_interval_str_ms_map",
        "_intervals_sorted",
        "_klines_last_closed",
        "_klines_store",
        "_klines_total_symbols_count",
        "_min_interval_ms",
        "_running",
        "_storage",
        "_tickers_snapshots",
        "_timer_task",
        "_timer_ticks_count",
        "_trades_buckets",
        "_trades_first_kline_skipped",
        "_trades_last_close",
        "_trades_waiter_latency_ms",
        "_trades_waiter_mode_enabled",
    )

    def __init__(
        self, broadcaster: Broadcaster, storage: Storage, channel: Channel
    ) -> None:
        """Init Aggregator instance."""
        self._broadcaster = broadcaster
        self._storage = storage
        self._running = False
        self._channel: Channel = channel

        intervals = settings.kline_intervals
        self._interval_ms_map: Dict["Interval", int] = {
            i: i.to_milliseconds() for i in intervals
        }
        self._interval_str_ms_map: Dict[str, int] = {
            i.to_bybit(): i.to_milliseconds() for i in intervals
        }
        self._intervals_sorted = tuple(sorted(self._interval_ms_map.values()))
        self._min_interval_ms = self._intervals_sorted[0]

        ### TRADES MODE VARS
        # symbol -> interval_ms -> bucket_end -> bucket
        # TODO: very deep struct, need to refactor
        self._trades_buckets: Dict[str, Dict[int, Dict[int, Dict[str, Any]]]] = {}

        # symbol -> interval_ms -> bool (first kline skipped)
        self._trades_first_kline_skipped: Dict[str, Dict[int, bool]] = {}

        # symbol -> interval_ms -> last close price
        self._trades_last_close: Dict[str, Dict[int, Any]] = {}

        # Configuration
        self._trades_waiter_mode_enabled = settings.aggregator_waiter_mode_enabled
        self._trades_waiter_latency_ms = settings.aggregator_waiter_latency_ms

        ### KLINES MODE VARS
        # symbol -> interval_ms -> full kline object
        self._klines_store: Dict[str, Dict[int, Dict[str, Any]]] = {}
        # interval_ms -> -> kline_start_ms -> list[full kline object]
        self._klines_last_closed: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}

        self._klines_total_symbols_count = len(settings.bybit_symbols)

        ### TICKERS
        # local cache: symbol -> last snapshot dict
        self._tickers_snapshots: dict[str, dict[str, Any]] = {}

        ### TIMER
        self._timer_task: asyncio.Task[None] | None = None
        self._timer_ticks_count = 0
        if logger.isEnabledFor(logging.INFO):
            if settings.klines_mode:
                logger.info(
                    "Aggregator initialized on channel: %s, intervals: %s, "
                    "mode: klines",
                    self._channel,
                    ",".join(str(i) for i in sorted(intervals)),
                )
            else:
                logger.info(
                    "Aggregator initialized on channel: %s, intervals: %s, "
                    "mode: trades, waiter_mode: %s, waiter_latency: %sms",
                    self._channel,
                    ", ".join(str(i) for i in sorted(intervals)),
                    self._trades_waiter_mode_enabled,
                    self._trades_waiter_latency_ms,
                )

    async def handle_trade(self, message: Dict[str, Any]) -> None:
        """Handle trade event and update OHLC buckets per bucket_start."""
        data = message["data"]

        for trade in sorted(data, key=lambda x: x["T"], reverse=False):
            ts: int = trade["T"]
            symbol: str = trade["s"]
            price: float = float(trade["p"])
            volume: float = float(trade["v"])

            for interval_ms in self._intervals_sorted:
                bucket_start = (ts // interval_ms) * interval_ms
                bucket_end = bucket_start + interval_ms

                interval_buckets = self._trades_buckets.setdefault(
                    symbol, {}
                ).setdefault(interval_ms, {})
                bucket = interval_buckets.get(bucket_start)

                if bucket is None:
                    trades_last_close_for_symbol = self._trades_last_close.get(
                        symbol, {}
                    )
                    last_close = trades_last_close_for_symbol.get(interval_ms)
                    bucket_open = last_close if last_close is not None else price

                    bucket = {
                        "start": bucket_start,
                        "end": bucket_end,
                        "o": bucket_open,
                        "c": price,
                        "h": price,
                        "l": price,
                        "v": volume,
                        "n": 1,
                    }
                    interval_buckets[bucket_start] = bucket

                else:
                    bucket["h"] = max(bucket["h"], price)
                    bucket["l"] = min(bucket["l"], price)
                    bucket["v"] += volume
                    bucket["n"] += 1
                    bucket["c"] = price

        if settings.enable_trades_stream:
            await self._broadcaster.consume(self._channel, "trades", data)

    async def handle_kline(self, message: Dict[str, Any]) -> None:
        """Handle incoming kline and update store."""
        topic = message["topic"]
        colon_idx = topic.find(".")
        if colon_idx < 0:
            logger.error(f"Malformed kline topic: {topic}")
            return

        try:
            _, interval_str, symbol = topic.split(".")
            interval_ms = self._interval_str_ms_map[interval_str]
        except Exception as e:
            logger.error(f"Malformed kline topic: {topic}, error: {e}")
            return

        klines_data = message["data"]
        if not klines_data:
            return

        symbol_store = self._klines_store.get(symbol)
        if symbol_store is None:
            symbol_store = {}
            self._klines_store[symbol] = symbol_store

        last_closed_all = self._klines_last_closed.get(interval_ms)
        if last_closed_all is None:
            last_closed_all = {}
            self._klines_last_closed[interval_ms] = last_closed_all

        # Overwrite with freshest data for this interval
        for kline in klines_data:
            kline["symbol"] = symbol
            if kline["confirm"]:
                if kline["start"] not in last_closed_all:
                    last_closed_all[kline["start"]] = []
                last_closed_all[kline["start"]].append(kline)
                continue
            symbol_store[interval_ms] = kline

    async def handle_ticker(self, message: Dict[str, Any]) -> None:
        """Handle ticker event and update snapshot."""
        topic = message["topic"]
        if not (isinstance(topic, str) and topic.startswith("tickers.")):
            return

        try:
            symbol = topic.split(".", 1)[1]
        except Exception:
            symbol = "unknown"

        typ = message["type"]
        tickers_data = cast("dict[str, Any]", message.get("data"))
        if not tickers_data:
            logger.warning(f"No 'data' field in tickers event: {message}")
            return

        snapshot = parse_numbers(
            self._process_ticker_snapshot(symbol, typ, tickers_data)
        )
        # Determine price (for broadcasting as well)
        price = (
            snapshot["markPrice"]
            if self._channel == "linear"
            else snapshot["lastPrice"]
        )

        # Broadcast snapshot via broadcaster
        if settings.enable_ticker_stream:
            await self._broadcaster.consume(self._channel, "ticker", [snapshot])

        if settings.enable_price_stream:
            await self._broadcaster.consume(
                self._channel, "price", [{"symbol": symbol, "price": price}]
            )

        await self._storage.process_ticker(self._channel, symbol, snapshot, price)

    async def start(self) -> None:
        """Start aggregator timer loop."""
        if self._running:
            return
        self._running = True
        self._timer_task = asyncio.create_task(self._timer_loop())
        logger.info(f"Aggregator started for channel: {self._channel}")

    async def stop(self) -> None:
        """Stop aggregator timer and cleanup."""
        if not self._running:
            return
        self._running = False
        if self._timer_task:
            self._timer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timer_task
        logger.info(f"Aggregator stopped for channel: {self._channel}")

    def _process_ticker_snapshot(
        self, symbol: str, typ: str, tickers_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Update in-memory ticker cache and return snapshot."""
        cache = self._tickers_snapshots
        cache_key = symbol
        if typ == "snapshot":
            cache[cache_key] = tickers_data
            return tickers_data
        if typ == "delta":
            item = cache.get(cache_key)
            if item is not None:
                item.update(tickers_data)
                return item
            cache[cache_key] = tickers_data
            return tickers_data
        return tickers_data

    async def _timer_loop(self) -> None:
        """Run timer loop to synchronize boundaries for the aggregator."""
        min_interval = self._min_interval_ms
        try:
            while self._running:
                now = int(time.time() * 1000)

                boundary = ((now // min_interval) + 1) * min_interval
                sleep_s = (boundary - now) * 0.001
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)

                if self._trades_waiter_mode_enabled:
                    await asyncio.sleep(self._trades_waiter_latency_ms / 1000)

                self._timer_ticks_count += 1
                if settings.klines_mode:
                    await self._wait_klines(boundary)
                else:
                    await self._close_klines(boundary)
        except asyncio.CancelledError:
            raise

    async def _close_klines(self, boundary_ms: int) -> None:
        """Close OHLC buckets and publish ready klines."""
        out: list[dict[str, Any]] = []

        for symbol, intervals in self._trades_buckets.items():
            first_kline_skipped = self._trades_first_kline_skipped.setdefault(
                symbol, {}
            )
            last_close_per_interval = self._trades_last_close.setdefault(symbol, {})

            for interval_ms in self._intervals_sorted:
                interval_buckets = intervals.get(interval_ms, {})

                if not interval_buckets:
                    last_close = last_close_per_interval.get(interval_ms)
                    if last_close is not None and boundary_ms % interval_ms == 0:
                        out.append(
                            {
                                "symbol": symbol,
                                "interval": interval_ms,
                                "timestamp": boundary_ms - interval_ms,
                                "open": last_close,
                                "high": last_close,
                                "low": last_close,
                                "close": last_close,
                                "volume": 0.0,
                                "trade_count": 0,
                            }
                        )
                    continue

                to_close = [
                    start
                    for start, bucket in interval_buckets.items()
                    if bucket["end"] <= boundary_ms
                ]

                for bucket_start in sorted(to_close):
                    bucket = interval_buckets.pop(bucket_start)

                    if not first_kline_skipped.get(interval_ms, False):
                        first_kline_skipped[interval_ms] = True
                        last_close_per_interval[interval_ms] = bucket["c"]
                        continue

                    next_bucket_start = bucket_start + interval_ms
                    next_bucket = interval_buckets.get(next_bucket_start)
                    if next_bucket is not None:
                        next_bucket["o"] = bucket["c"]

                    last_close_per_interval[interval_ms] = bucket["c"]

                    out.append(
                        {
                            "symbol": symbol,
                            "interval": interval_ms,
                            "timestamp": bucket["start"],
                            "open": bucket["o"],
                            # recalculate high and low as max/min of o, h, c, l
                            "high": max(
                                bucket["o"], bucket["h"], bucket["c"], bucket["l"]
                            ),
                            "low": min(
                                bucket["o"], bucket["h"], bucket["c"], bucket["l"]
                            ),
                            "close": bucket["c"],
                            "volume": bucket["v"],
                            "trade_count": bucket["n"],
                        }
                    )

        if out:
            logger.info(
                f"Closing {len(out)} klines at boundary {boundary_ms} "
                f"in channel {self._channel}"
            )
            await self._broadcaster.consume(self._channel, "klines", out)
            await self._storage.process_klines(self._channel, out)

    async def _wait_klines(self, boundary_ms: int) -> None:
        """Wait for all closed klines, then publish."""
        out: list[dict[str, Any]] = []

        for interval_ms in self._intervals_sorted:
            if boundary_ms % interval_ms != 0:
                continue

            start_boundary = boundary_ms - interval_ms
            waited = 0.0

            while True:
                closed_by_interval = self._klines_last_closed.setdefault(
                    interval_ms, {}
                )
                closed = closed_by_interval.get(start_boundary, [])
                if len(closed) >= self._klines_total_symbols_count:
                    break
                if waited >= KLINE_MAX_WAIT_TIME:
                    logger.warning(
                        f"_wait_klines timeout: Received "
                        f"{len(closed)}/{self._klines_total_symbols_count} symbols for "
                        f"interval {interval_ms} at start={start_boundary}; "
                        f"continuing with available."
                    )
                    break
                await asyncio.sleep(KLINE_SLEEP_TIME)
                waited += KLINE_SLEEP_TIME

            closed_klines = self._klines_last_closed[interval_ms].pop(
                start_boundary, []
            )

            if closed_klines:
                out.extend(closed_klines)

        if out:
            logger.info(
                f"Collect {len(out)} closed klines at boundary "
                f"{boundary_ms} in channel {self._channel} "
            )
            await self._broadcaster.consume(self._channel, "klines", out)
            await self._storage.process_klines(self._channel, out)
