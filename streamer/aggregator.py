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
        "_tickers_buckets",
        "_tickers_first_kline_skipped",
        "_tickers_last_boundary",
        "_tickers_last_close",
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
        # Core components
        self._broadcaster = broadcaster
        self._storage = storage
        self._channel: Channel = channel
        self._running = False

        # Interval mappings
        intervals = settings.kline_intervals
        self._interval_ms_map: Dict["Interval", int] = {
            i: i.to_milliseconds() for i in intervals
        }
        self._interval_str_ms_map: Dict[str, int] = {
            i.to_bybit(): i.to_milliseconds() for i in intervals
        }
        self._intervals_sorted = tuple(sorted(self._interval_ms_map.values()))
        self._min_interval_ms = self._intervals_sorted[0]

        # --- Trades Mode Vars ---
        # Buckets for trades aggregation: symbol -> interval_ms -> bucket_end -> bucket
        # TODO: very deep struct, need to refactor
        self._trades_buckets: Dict[str, Dict[int, Dict[int, Dict[str, Any]]]] = {}
        # Track first kline skipped flag: symbol -> interval_ms -> bool
        self._trades_first_kline_skipped: Dict[str, Dict[int, bool]] = {}
        # Last close price for trades: symbol -> interval_ms -> price
        self._trades_last_close: Dict[str, Dict[int, Any]] = {}
        # Trades mode config
        self._trades_waiter_mode_enabled = settings.aggregator_waiter_mode_enabled
        self._trades_waiter_latency_ms = settings.aggregator_waiter_latency_ms

        # --- Klines Mode Vars ---
        # Local kline store: symbol -> interval_ms -> kline object
        self._klines_store: Dict[str, Dict[int, Dict[str, Any]]] = {}
        # Last closed klines: interval_ms -> kline_start_ms -> list[kline object]
        self._klines_last_closed: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}
        # Klines mode static info
        self._klines_total_symbols_count = len(settings.bybit_symbols)

        # --- Tickers Vars ---
        # Local cache: symbol -> last snapshot dict
        self._tickers_snapshots: dict[str, dict[str, Any]] = {}
        # Buckets: symbol -> interval_ms -> bucket_start -> bucket
        self._tickers_buckets: Dict[str, Dict[int, Dict[int, Dict[str, Any]]]] = {}
        # First kline skipped for ticker: symbol -> interval_ms -> bool
        self._tickers_first_kline_skipped: Dict[str, Dict[int, bool]] = {}
        # Last close values: symbol -> interval_ms -> dict[field, value]
        self._tickers_last_close: Dict[str, Dict[int, dict[str, float]]] = {}
        # Last closed boundary to drop tickers data in past time
        self._tickers_last_boundary: dict[int, int] = {}

        # --- Timer Vars ---
        self._timer_task: asyncio.Task[None] | None = None
        self._timer_ticks_count = 0

        # --- Logging on init ---
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

        for trade in data:
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
        ts = message["ts"]
        tickers_data = cast("dict[str, Any]", message.get("data"))
        if not tickers_data:
            logger.warning(f"No 'data' field in tickers event: {message}")
            return

        snapshot = parse_numbers(
            self._process_ticker_snapshot(symbol, typ, ts, tickers_data)
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

        if settings.enable_tickers_kline_stream:
            await self._process_ticker_kline(symbol, snapshot, ts)

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
        self, symbol: str, typ: str, ts: int, tickers_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Update in-memory ticker cache and return snapshot."""
        tickers_data["ts"] = ts
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

    def _get_numeric_fields_for_channel(self) -> list[str]:
        """Get numeric fields that should be tracked for OHLC based on channel."""
        fields = ["turnover24h", "volume24h"]
        if self._channel != "spot":
            fields.extend(["openInterest", "fundingRate"])
        return fields

    def _create_ohlc_bucket(
        self,
        interval_ms: int,
        bucket_start: int,
        bucket_end: int,
        snapshot: dict[str, Any],
        numeric_fields: list[str],
    ) -> dict[str, Any]:
        """Create a new OHLC bucket for ticker data."""
        ohlc_data = {}
        last_close_per_field = self._tickers_last_close.get(snapshot["symbol"], {}).get(
            interval_ms
        )

        for field in numeric_fields:
            if field in snapshot:
                value = snapshot[field]
                open_val = value
                # If last_close exists for field, use it as open
                if last_close_per_field is not None and field in last_close_per_field:
                    open_val = last_close_per_field[field]
                ohlc_data[field] = {
                    "o": open_val,
                    "h": value,
                    "l": value,
                    "c": value,
                }

        bucket: dict[str, Any] = {
            "start": bucket_start,
            "end": bucket_end,
            **ohlc_data,
            "n": 1,
        }
        return bucket

    def _update_ohlc_bucket(
        self,
        bucket: dict[str, Any],
        snapshot: dict[str, Any],
        numeric_fields: list[str],
    ) -> None:
        """Update existing OHLC bucket with new ticker data."""
        for field in numeric_fields:
            if field in snapshot and field in bucket:
                value = snapshot[field]
                bucket[field]["c"] = value
                bucket[field]["h"] = max(bucket[field]["h"], value)
                bucket[field]["l"] = min(bucket[field]["l"], value)
        bucket["n"] += 1

    async def _process_ticker_kline(
        self, symbol: str, snapshot: dict[str, Any], ts: int
    ) -> None:
        """Process ticker snapshot for kline aggregation."""
        numeric_fields = self._get_numeric_fields_for_channel()

        for interval_ms in self._intervals_sorted:
            bucket_start = (ts // interval_ms) * interval_ms
            bucket_end = bucket_start + interval_ms

            # Get or create buckets for this symbol and interval
            tickers_buckets = self._tickers_buckets.setdefault(symbol, {}).setdefault(
                interval_ms, {}
            )
            bucket = tickers_buckets.get(bucket_start)

            last_boundary = self._tickers_last_boundary.get(interval_ms)

            # Important: pass past time data
            if last_boundary is not None and bucket_end <= last_boundary:
                continue

            if bucket is None:
                # Create new bucket
                new_bucket = self._create_ohlc_bucket(
                    interval_ms, bucket_start, bucket_end, snapshot, numeric_fields
                )
                tickers_buckets[bucket_start] = new_bucket
            else:
                # Update existing bucket
                self._update_ohlc_bucket(bucket, snapshot, numeric_fields)

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

                if settings.enable_tickers_kline_stream:
                    await self._close_tickers_klines(boundary)

                if self._trades_waiter_mode_enabled and not settings.klines_mode:
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
                        # Also update open price of new next bucket if it exists
                        next_bucket = interval_buckets.get(bucket_start + interval_ms)
                        if next_bucket is not None:
                            next_bucket["o"] = bucket["c"]
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
                            # todo: remove recalc after validate
                            "high": max(
                                bucket["o"], bucket["h"], bucket["l"], bucket["c"]
                            ),
                            "low": min(
                                bucket["o"], bucket["h"], bucket["l"], bucket["c"]
                            ),
                            "close": bucket["c"],
                            "volume": bucket["v"],
                            "trade_count": bucket["n"],
                        }
                    )

        if out:
            logger.info(
                f"Closing {len(out)}/{self._klines_total_symbols_count} "
                f"klines at boundary {boundary_ms} in channel {self._channel}"
            )
            await self._broadcaster.consume(self._channel, "klines", out)
            await self._storage.process_klines(self._channel, out)

    async def _close_tickers_klines(self, boundary_ms: int) -> None:
        """Close ticker OHLC buckets and publish ready ticker klines."""
        out: list[dict[str, Any]] = []

        for symbol, intervals in self._tickers_buckets.items():
            first_kline_skipped = self._tickers_first_kline_skipped.setdefault(
                symbol, {}
            )
            last_close_per_interval = self._tickers_last_close.setdefault(symbol, {})

            for interval_ms in self._intervals_sorted:
                self._tickers_last_boundary[interval_ms] = boundary_ms

                interval_buckets = intervals.get(interval_ms, {})

                if not interval_buckets:
                    # Create empty kline if we have last close values
                    last_close_data = last_close_per_interval.get(interval_ms)
                    if last_close_data is not None and boundary_ms % interval_ms == 0:
                        kline_data: dict[str, Any] = {
                            "symbol": symbol,
                            "interval": interval_ms,
                            "timestamp": boundary_ms - interval_ms,
                            "n": 0,
                        }
                        # Add OHLC data for each field using last close values
                        for field, value in last_close_data.items():
                            kline_data[field] = {
                                "o": value,
                                "h": value,
                                "l": value,
                                "c": value,
                            }
                        out.append(kline_data)
                    continue

                to_close = [
                    start
                    for start, bucket in interval_buckets.items()
                    if bucket["end"] <= boundary_ms
                ]

                for bucket_start in sorted(to_close):
                    bucket = interval_buckets.pop(bucket_start)
                    bucket_data = self._process_single_ticker_bucket(
                        symbol,
                        interval_ms,
                        bucket_start,
                        bucket,
                        interval_buckets,
                        first_kline_skipped,
                        last_close_per_interval,
                    )
                    if bucket_data:
                        out.append(bucket_data)

        if out:
            logger.info(
                f"Closing {len(out)}/{self._klines_total_symbols_count} ticker klines "
                f"at boundary {boundary_ms} in channel {self._channel}"
            )
            await self._broadcaster.consume(self._channel, "tickers-klines", out)
            await self._storage.process_tickers_klines(self._channel, out)

    def _process_single_ticker_bucket(
        self,
        symbol: str,
        interval_ms: int,
        bucket_start: int,
        bucket: dict[str, Any],
        interval_buckets: dict[int, dict[str, Any]],
        first_kline_skipped: dict[int, bool],
        last_close_per_interval: dict[int, dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Process a single ticker bucket for closing."""
        numeric_fields = self._get_numeric_fields_for_channel()

        # Handle first kline: skip and initialize last_close
        if self._handle_first_ticker_kline(
            interval_ms,
            bucket,
            first_kline_skipped,
            last_close_per_interval,
            numeric_fields,
            symbol,
            bucket_start,
            interval_buckets,
        ):
            return None

        # Update next bucket open prices and last close data
        self._update_ticker_bucket_state(
            interval_ms,
            bucket,
            last_close_per_interval,
            numeric_fields,
            symbol,
            bucket_start,
            interval_buckets,
        )

        # Create and return kline data
        return self._create_ticker_kline_data(
            symbol, interval_ms, bucket, numeric_fields
        )

    def _handle_first_ticker_kline(  # noqa: PLR0913
        self,
        interval_ms: int,
        bucket: dict[str, Any],
        first_kline_skipped: dict[int, bool],
        last_close_per_interval: dict[int, dict[str, Any]],
        numeric_fields: list[str],
        symbol: str,
        bucket_start: int,
        interval_buckets: dict[int, dict[str, Any]],
    ) -> bool:
        """Handle first kline for ticker interval. Returns True if skipped."""
        if first_kline_skipped.get(interval_ms, False):
            return False

        first_kline_skipped[interval_ms] = True
        last_close_per_interval.setdefault(interval_ms, {}).update(
            {field: bucket[field]["c"] for field in numeric_fields if field in bucket}
        )

        # Log missing fields
        for field in numeric_fields:
            if field not in bucket:
                logger.warning(
                    f"Field {field} missing in first ticker bucket for "
                    f"symbol={symbol} interval={interval_ms}"
                )

        # Set next bucket open prices
        next_bucket = interval_buckets.get(bucket_start + interval_ms)
        if next_bucket:
            for field in numeric_fields:
                if field in bucket and field in next_bucket:
                    next_bucket[field]["o"] = bucket[field]["c"]

        return True

    def _update_ticker_bucket_state(
        self,
        interval_ms: int,
        bucket: dict[str, Any],
        last_close_per_interval: dict[int, dict[str, Any]],
        numeric_fields: list[str],
        symbol: str,
        bucket_start: int,
        interval_buckets: dict[int, dict[str, Any]],
    ) -> None:
        """Update next bucket open prices and last close data."""
        # Set next bucket open prices
        next_bucket = interval_buckets.get(bucket_start + interval_ms)
        if next_bucket:
            for field in numeric_fields:
                if field in bucket and field in next_bucket:
                    next_bucket[field]["o"] = bucket[field]["c"]

        # Update last close values
        last_close_per_interval.setdefault(interval_ms, {}).update(
            {field: bucket[field]["c"] for field in numeric_fields if field in bucket}
        )

        # Log missing fields
        for field in numeric_fields:
            if field not in bucket:
                logger.warning(
                    f"Field {field} missing in ticker bucket for "
                    f"symbol={symbol} interval={interval_ms}"
                )

    def _create_ticker_kline_data(
        self,
        symbol: str,
        interval_ms: int,
        bucket: dict[str, Any],
        numeric_fields: list[str],
    ) -> dict[str, Any]:
        """Create kline data dict from ticker bucket."""
        kline_data = {
            "symbol": symbol,
            "interval": interval_ms,
            "timestamp": bucket["start"],
            "n": bucket["n"],
        }
        kline_data.update(
            {field: dict(bucket[field]) for field in numeric_fields if field in bucket}
        )
        return kline_data

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
