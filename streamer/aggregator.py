"""Aggregator timer + OHLC builder (ultra-fast)."""

import asyncio
import contextlib
import logging
import time
from typing import TYPE_CHECKING, Any, Dict

from streamer.broadcaster import Broadcaster
from streamer.settings import settings

if TYPE_CHECKING:
    from streamer.types import Interval

logger = logging.getLogger(__name__)


class Aggregator:
    """Extremely fast OHLC aggregator synchronized to interval boundaries."""

    __slots__ = (
        "_broadcaster",
        "_buckets",
        "_interval_ms_map",
        "_intervals_sorted",
        "_last_close",
        "_min_interval_ms",
        "_running",
        "_timer_task",
        "_timer_ticks_count",
        "_waiter_mode_enabled",
    )

    def __init__(self, broadcaster: Broadcaster) -> None:
        """Initialize the Aggregator with the provided broadcaster."""
        self._broadcaster = broadcaster
        self._running = False
        self._timer_task: asyncio.Task[None] | None = None

        intervals = settings.kline_intervals
        self._interval_ms_map: Dict["Interval", int] = {
            i: i.to_milliseconds() for i in intervals
        }
        self._intervals_sorted = tuple(sorted(self._interval_ms_map.values()))
        self._min_interval_ms = self._intervals_sorted[0]

        # symbol -> interval_ms -> bucket
        self._buckets: Dict[str, Dict[int, Dict[str, Any]]] = {}

        # symbol -> interval_ms -> last_close
        self._last_close: Dict[str, Dict[int, float]] = {}

        # Configuration
        self._waiter_mode_enabled = settings.aggregator_waiter_mode_enabled

        self._timer_ticks_count = 0

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Aggregator initialized with intervals: %s, waiter_mode: %s",
                ",".join(str(i) for i in sorted(intervals)),
                self._waiter_mode_enabled,
            )

    async def handle_trade(self, message: Dict[str, Any]) -> None:
        """Process a new trade message to update OHLC buckets."""
        data = message["data"]

        symbol_buckets_cache: dict[str, dict[int, dict[str, Any]]] = {}
        last_close_cache: dict[str, dict[int, float]] = {}

        for trade in data:
            ts: int = trade["T"]
            symbol: str = trade["s"]
            price: float = float(trade["p"])
            volume: float = float(trade["v"])

            # кешируем dict для быстрого доступа на все интервалы
            symbol_buckets = symbol_buckets_cache.setdefault(
                symbol, self._buckets.setdefault(symbol, {})
            )
            symbol_last_close = last_close_cache.setdefault(
                symbol, self._last_close.setdefault(symbol, {})
            )

            for interval_ms in self._intervals_sorted:
                bucket_start = (ts // interval_ms) * interval_ms
                bucket_end = bucket_start + interval_ms

                bucket = symbol_buckets.get(interval_ms)

                # NOTE: This code need to recalc close by delayed data
                if (
                    bucket is None
                    and bucket_start
                    < int(time.time() * 1000) // interval_ms * interval_ms
                ):
                    symbol_last_close.get(interval_ms, price)
                    symbol_last_close[interval_ms] = price
                    continue

                if bucket is None or bucket["end"] != bucket_end:
                    # открываем новую свечу
                    last_close = symbol_last_close.get(interval_ms)
                    o = price if last_close is None else last_close
                    symbol_buckets[interval_ms] = {
                        "start": bucket_start,
                        "end": bucket_end,
                        "o": o,
                        "h": price,
                        "l": price,
                        "c": price,
                        "v": volume,
                        "n": 1,
                    }
                else:
                    bucket["h"] = max(bucket["h"], price, bucket["o"], bucket["c"])
                    bucket["l"] = min(bucket["l"], price, bucket["o"], bucket["c"])
                    bucket["c"] = price
                    bucket["v"] += volume
                    bucket["n"] += 1

    async def start(self) -> None:
        """Start the aggregator's timer loop."""
        if self._running:
            return
        self._running = True
        self._timer_task = asyncio.create_task(self._timer_loop())
        logger.info("Aggregator started")

    async def stop(self) -> None:
        """Stop the aggregator's timer loop and cleanup."""
        if not self._running:
            return
        self._running = False
        if self._timer_task:
            self._timer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timer_task
        logger.info("Aggregator stopped")

    async def _timer_loop(self) -> None:
        min_interval = self._min_interval_ms
        try:
            while self._running:
                now = int(time.time() * 1000)
                boundary = ((now // min_interval) + 1) * min_interval
                sleep_s = (boundary - now) * 0.001
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)

                if self._waiter_mode_enabled:
                    await asyncio.sleep(0.08)  # 80ms extra delay in waiter mode

                self._timer_ticks_count += 1
                await self._close_candles(boundary)
        except asyncio.CancelledError:
            raise

    async def _close_candles(self, boundary_ms: int) -> None:
        out: list[dict[str, Any]] = []

        for symbol, intervals in self._buckets.items():
            last_close = self._last_close.setdefault(symbol, {})

            for interval_ms in self._intervals_sorted:
                if boundary_ms % interval_ms != 0:
                    continue

                start = boundary_ms - interval_ms
                bucket = intervals.get(interval_ms)

                if bucket is None:
                    prev = last_close.get(interval_ms)
                    if prev is None:
                        continue
                    out.append(
                        {
                            "symbol": symbol,
                            "interval": interval_ms,
                            "timestamp": start,
                            "open": prev,
                            "high": prev,
                            "low": prev,
                            "close": prev,
                            "volume": 0.0,
                            "trade_count": 0,
                        }
                    )
                    continue

                intervals.pop(interval_ms, None)
                last_close[interval_ms] = bucket["c"]
                out.append(
                    {
                        "symbol": symbol,
                        "interval": interval_ms,
                        "timestamp": start,
                        "open": bucket["o"],
                        "high": bucket["h"],
                        "low": bucket["l"],
                        "close": bucket["c"],
                        "volume": bucket["v"],
                        "trade_count": bucket["n"],
                    }
                )

        if out:
            logger.debug(f"Closing {len(out)} candles at boundary {boundary_ms}")
            await self._broadcaster.handle(out)
