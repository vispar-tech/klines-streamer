"""Ultra-fast OHLC aggregator; timer and builder."""

import asyncio
import contextlib
import logging
import time
from typing import Any, Dict, List, Set, cast

from streamer.broadcaster import Broadcaster
from streamer.settings import settings
from streamer.storage import Storage
from streamer.types import Channel, Interval
from streamer.utils.parse import parse_numbers

logger = logging.getLogger(__name__)

KLINE_MAX_WAIT_TIME = 20.0
KLINE_SLEEP_TIME = 0.01


class Aggregator:
    """OHLC aggregator with interval synchronization for trades and tickers."""

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
        "_tickers_numeric_fields",
        "_tickers_snapshots",
        "_timer_task",
        "_timer_ticks_count",
        "_trades_buckets",
        "_trades_first_kline_skipped",
        "_trades_last_boundary",
        "_trades_last_close",
        "_trades_waiter_latency_ms",
        "_trades_waiter_mode_enabled",
    )

    def __init__(
        self, broadcaster: Broadcaster, storage: Storage, channel: Channel
    ) -> None:
        """Initialize Aggregator instance with all necessary state."""
        # Core components setup
        self._broadcaster = broadcaster
        self._storage = storage
        self._channel: Channel = channel
        self._running = False

        # Initialize interval mappings and config
        intervals = settings.kline_intervals
        self._interval_ms_map: Dict["Interval", int] = {
            i: i.to_milliseconds() for i in intervals
        }
        self._interval_str_ms_map: Dict[str, int] = {
            i.to_bybit(): i.to_milliseconds() for i in intervals
        }
        self._intervals_sorted = tuple(sorted(self._interval_ms_map.values()))
        self._min_interval_ms = self._intervals_sorted[0]

        # Initialize trades mode state
        self._trades_buckets: Dict[str, Dict[int, Dict[int, Dict[str, Any]]]] = {}
        self._trades_first_kline_skipped: Dict[str, Dict[int, bool]] = {}
        self._trades_last_close: Dict[str, Dict[int, Any]] = {}
        self._trades_last_boundary: dict[int, int] = {}
        # For waiter mode
        self._trades_waiter_mode_enabled = settings.aggregator_waiter_mode_enabled
        self._trades_waiter_latency_ms = settings.aggregator_waiter_latency_ms

        # Initialize klines mode state
        self._klines_store: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self._klines_last_closed: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}
        self._klines_total_symbols_count = len(settings.exchange_symbols)

        # Initialize tickers state
        self._tickers_snapshots: dict[str, dict[str, Any]] = {}
        self._tickers_buckets: Dict[str, Dict[int, Dict[int, Dict[str, Any]]]] = {}
        self._tickers_first_kline_skipped: Dict[str, Dict[int, bool]] = {}
        self._tickers_last_close: Dict[str, Dict[int, dict[str, float]]] = {}
        self._tickers_last_boundary: dict[int, int] = {}

        # Pre-compute numeric fields for this channel
        self._tickers_numeric_fields = ["turnover24h", "volume24h"]
        if self._channel != "spot":
            self._tickers_numeric_fields.extend(["openInterest", "fundingRate"])

        # Initialize timer state
        self._timer_task: asyncio.Task[None] | None = None
        self._timer_ticks_count = 0

        # Log initialization info
        self._log_initialization_info(intervals)

    def _log_initialization_info(self, intervals: Set[Interval]) -> None:
        """Log aggregator initialization details with all enabled features."""
        if not logger.isEnabledFor(logging.INFO):
            return

        # Prepare basic info
        interval_str = ",".join(str(i) for i in sorted(intervals))
        symbols_count = len(settings.exchange_symbols)
        mode_str = "trades"
        storage_str = "enabled" if settings.storage_enabled else "disabled"

        # Build streams list
        streams = [
            name
            for flag, name in [
                (settings.enable_ticker_stream, "ticker"),
                (settings.enable_price_stream, "price"),
                (settings.enable_tickers_kline_stream, "tickers-klines"),
                (settings.enable_trades_stream, "trades"),
            ]
            if flag
        ]
        streams_str = ",".join(streams) if streams else "none"

        # Build mode details
        waiter_info = ""
        if self._trades_waiter_mode_enabled:
            waiter_info = f" (waiter: {self._trades_waiter_latency_ms}ms)"

        # Log main info
        logger.info(
            "Aggregator initialized - channel: %s, symbols: %d, intervals: %s, "
            "mode: %s%s, streams: %s, storage: %s",
            self._channel,
            symbols_count,
            interval_str,
            mode_str,
            waiter_info,
            streams_str,
            storage_str,
        )

        # Log additional config if needed
        if settings.enable_spot_stream or len(streams) > 2:
            extra: list[str] = []
            if settings.enable_spot_stream:
                extra.append("spot_stream: enabled")
            if len(streams) > 2:
                extra.append(f"active_streams: {len(streams)}")
            if extra:
                logger.info("Additional config: %s", ", ".join(extra))

    async def handle_trade(self, message: Dict[str, Any]) -> None:
        """Handle trade event and update OHLC buckets per bucket_start."""
        trades_data = message["data"]
        trades_buckets = self._trades_buckets
        trades_last_close = self._trades_last_close
        trades_last_boundary = self._trades_last_boundary

        for trade in trades_data:
            # Extract trade data once
            trade_ts = int(trade["T"])
            trade_symbol = trade["s"]
            trade_price = float(trade["p"])
            trade_volume = float(trade["v"])

            # Get symbol-specific storage
            symbol_buckets = trades_buckets.setdefault(trade_symbol, {})
            symbol_last_close = trades_last_close.get(trade_symbol, {})

            for interval_ms in self._intervals_sorted:
                # Calculate bucket boundaries
                bucket_start_ts = (trade_ts // interval_ms) * interval_ms
                bucket_end_ts = bucket_start_ts + interval_ms

                # Use the last processed boundary for trades (like tickers)
                last_processed_boundary = trades_last_boundary.get(interval_ms)
                if (
                    last_processed_boundary is not None
                    and bucket_end_ts <= last_processed_boundary
                ):
                    if logger.isEnabledFor(logging.WARN):
                        logger.warning(
                            "Trade over boundary for symbol=%s, interval=%s: "
                            "last_boundary=%s, bucket_end=%s, trade=%r",
                            trade_symbol,
                            interval_ms,
                            last_processed_boundary,
                            bucket_end_ts,
                            trade,
                        )
                    continue

                # Get interval-specific storage
                interval_buckets = symbol_buckets.setdefault(interval_ms, {})
                existing_bucket = interval_buckets.get(bucket_start_ts)

                if existing_bucket is None:
                    # Create new bucket - use last close as open price if available
                    bucket_open_price = symbol_last_close.get(interval_ms, trade_price)

                    new_bucket = {
                        "start": bucket_start_ts,
                        "end": bucket_end_ts,
                        "o": bucket_open_price,
                        "c": trade_price,
                        "h": trade_price,
                        "l": trade_price,
                        "v": trade_volume,
                        "n": 1,
                    }
                    interval_buckets[bucket_start_ts] = new_bucket
                else:
                    # Update existing bucket
                    existing_bucket["h"] = max(existing_bucket["h"], trade_price)
                    existing_bucket["l"] = min(existing_bucket["l"], trade_price)
                    existing_bucket["v"] += trade_volume
                    existing_bucket["n"] += 1
                    existing_bucket["c"] = trade_price

        if settings.enable_trades_stream:
            await self._broadcaster.consume(self._channel, "trades", trades_data)

    async def handle_kline(self, message: Dict[str, Any]) -> None:
        """Handle incoming kline and update store."""
        kline_topic = message["topic"]

        # Parse topic components
        try:
            _, interval_str, kline_symbol = kline_topic.split(".")
            interval_ms = self._interval_str_ms_map[interval_str]
        except Exception as e:
            logger.error(
                "Malformed kline topic: %s, error: %s",
                kline_topic,
                e,
            )
            return

        # Get kline data
        kline_messages = message["data"]
        if not kline_messages:
            return

        # Get storage references
        klines_store = self._klines_store
        klines_last_closed = self._klines_last_closed

        # Get or create symbol storage
        symbol_storage = klines_store.get(kline_symbol)
        if symbol_storage is None:
            symbol_storage = {}
            klines_store[kline_symbol] = symbol_storage

        # Get or create closed klines storage for this interval
        closed_klines_store = klines_last_closed.get(interval_ms)
        if closed_klines_store is None:
            closed_klines_store = {}
            klines_last_closed[interval_ms] = closed_klines_store

        # Process each kline message
        for kline_msg in kline_messages:
            kline_msg["symbol"] = kline_symbol

            if kline_msg["confirm"]:
                # Store confirmed kline in closed store
                kline_start = kline_msg["start"]
                if kline_start not in closed_klines_store:
                    closed_klines_store[kline_start] = []
                closed_klines_store[kline_start].append(kline_msg)
            else:
                # Update symbol storage with unconfirmed kline
                symbol_storage[interval_ms] = kline_msg

    async def handle_ticker(self, message: Dict[str, Any]) -> None:
        """Handle ticker event and update all related streams."""
        ticker_topic = message["topic"]

        # Extract symbol from topic
        try:
            ticker_symbol = ticker_topic.split(".", 1)[1]
        except Exception:
            ticker_symbol = "unknown"

        # Extract message data
        message_type = message["type"]
        timestamp = message["ts"]
        ticker_data = cast("dict[str, Any]", message.get("data"))

        if not ticker_data:
            logger.warning(
                "No 'data' field in tickers event: %s",
                message,
            )
            return

        # Process ticker snapshot
        ticker_snapshot = parse_numbers(
            self._process_ticker_snapshot(
                ticker_symbol, message_type, timestamp, ticker_data
            )
        )

        # Get references for faster access
        broadcaster = self._broadcaster
        storage = self._storage
        channel = self._channel

        # Calculate broadcast price
        broadcast_price = (
            ticker_snapshot["markPrice"]
            if channel == "linear"
            else ticker_snapshot["lastPrice"]
        )

        # Broadcast ticker data if enabled
        if settings.enable_ticker_stream:
            await broadcaster.consume(channel, "ticker", [ticker_snapshot])

        # Broadcast price data if enabled
        if settings.enable_price_stream:
            await broadcaster.consume(
                channel, "price", [{"symbol": ticker_symbol, "price": broadcast_price}]
            )

        # Process ticker kline aggregation if enabled
        if settings.enable_tickers_kline_stream:
            await self._process_ticker_kline(ticker_symbol, ticker_snapshot, timestamp)

        # Store ticker data
        await storage.process_ticker(
            channel, ticker_symbol, ticker_snapshot, broadcast_price
        )

    async def start(self) -> None:
        """Start aggregator timer loop."""
        if self._running:
            return

        # Update state and start timer task
        self._running = True
        self._timer_task = asyncio.create_task(self._timer_loop())

        # Log successful start
        channel_name = self._channel
        logger.info("Aggregator started for channel: %s", channel_name)

    async def stop(self) -> None:
        """Stop aggregator timer and cleanup resources."""
        if not self._running:
            return

        # Update state
        self._running = False

        # Cancel and cleanup timer task if exists
        timer_task = self._timer_task
        if timer_task is not None:
            timer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer_task

            # Clear reference
            self._timer_task = None

        # Log successful stop
        channel_name = self._channel
        logger.info("Aggregator stopped for channel: %s", channel_name)

    def _process_ticker_snapshot(
        self,
        ticker_symbol: str,
        message_type: str,
        timestamp: int,
        ticker_data: dict[str, Any],
    ) -> dict[str, Any]:
        """Update in-memory ticker cache and return snapshot."""
        # Add timestamp to ticker data
        ticker_data["ts"] = timestamp

        # Get cache reference for faster access
        ticker_cache = self._tickers_snapshots

        if message_type == "snapshot":
            # Full snapshot - replace entire entry
            ticker_cache[ticker_symbol] = ticker_data
            return ticker_data

        if message_type == "delta":
            # Delta update - merge with existing or create new
            existing_entry = ticker_cache.get(ticker_symbol)
            if existing_entry is not None:
                existing_entry.update(ticker_data)
                return existing_entry
            # No existing entry, store as new
            ticker_cache[ticker_symbol] = ticker_data
            return ticker_data

        # Unknown message type - return as-is
        return ticker_data

    def _create_ohlc_bucket(
        self,
        interval_ms: int,
        bucket_start: int,
        bucket_end: int,
        ticker_symbol: str,
        snapshot: dict[str, Any],
        numeric_fields: list[str],
    ) -> dict[str, Any]:
        """Create a new OHLC bucket for ticker data."""
        # Get references for faster access
        ticker_last_close = self._tickers_last_close
        fields_data = {}

        # Get last close values for this symbol and interval
        last_close_values = ticker_last_close.get(ticker_symbol, {}).get(interval_ms)

        # Create OHLC data for each numeric field
        for field_name in numeric_fields:
            if field_name in snapshot:
                current_value = snapshot[field_name]
                # Use last close as open price if available, otherwise current value
                open_value = (
                    last_close_values.get(field_name, current_value)
                    if last_close_values
                    else current_value
                )

                fields_data[field_name] = {
                    "o": open_value,
                    "h": current_value,
                    "l": current_value,
                    "c": current_value,
                }

        return {
            "start": bucket_start,
            "end": bucket_end,
            **fields_data,
            "n": 1,
        }

    def _update_ohlc_bucket(
        self,
        bucket: dict[str, Any],
        snapshot: dict[str, Any],
        numeric_fields: list[str],
    ) -> None:
        """Update existing OHLC bucket with new ticker data."""
        # Update OHLC values for each numeric field present in snapshot
        for field_name in numeric_fields:
            if field_name in snapshot and field_name in bucket:
                current_value = snapshot[field_name]
                field_bucket = bucket[field_name]

                # Update close price
                field_bucket["c"] = current_value
                # Update high and low
                field_bucket["h"] = max(field_bucket["h"], current_value)
                field_bucket["l"] = min(field_bucket["l"], current_value)

        # Increment tick count
        bucket["n"] += 1

    async def _process_ticker_kline(
        self, ticker_symbol: str, ticker_snapshot: dict[str, Any], timestamp: int
    ) -> None:
        """Process ticker snapshot for kline aggregation across all intervals."""
        # Add symbol to snapshot for bucket creation
        numeric_fields = self._tickers_numeric_fields

        # Get references for faster access
        tickers_buckets_ref = self._tickers_buckets
        last_boundary_ref = self._tickers_last_boundary

        # Process each configured interval
        for interval_ms in self._intervals_sorted:
            # Calculate bucket boundaries for this timestamp
            bucket_start_ts = (timestamp // interval_ms) * interval_ms
            bucket_end_ts = bucket_start_ts + interval_ms

            # Skip if this bucket end is in the past (already processed)
            last_processed_boundary = last_boundary_ref.get(interval_ms)
            if (
                last_processed_boundary is not None
                and bucket_end_ts <= last_processed_boundary
            ):
                continue

            # Get or create bucket storage for this symbol and interval
            symbol_buckets = tickers_buckets_ref.setdefault(ticker_symbol, {})
            interval_buckets = symbol_buckets.setdefault(interval_ms, {})
            existing_bucket = interval_buckets.get(bucket_start_ts)

            if existing_bucket is None:
                # Create new bucket with OHLC data
                new_bucket = self._create_ohlc_bucket(
                    interval_ms,
                    bucket_start_ts,
                    bucket_end_ts,
                    ticker_symbol,
                    ticker_snapshot,
                    numeric_fields,
                )
                interval_buckets[bucket_start_ts] = new_bucket
            else:
                # Update existing bucket with new data
                self._update_ohlc_bucket(
                    existing_bucket, ticker_snapshot, numeric_fields
                )

    async def _timer_loop(self) -> None:
        """Run timer loop to synchronize boundaries for the aggregator."""
        # Get references for faster access
        min_interval_ms = self._min_interval_ms
        waiter_enabled = self._trades_waiter_mode_enabled
        waiter_latency_ms = self._trades_waiter_latency_ms
        tickers_enabled = settings.enable_tickers_kline_stream

        try:
            while self._running:
                # Get current time in milliseconds
                current_time_ms = int(time.time() * 1000)

                # Calculate next boundary and sleep time
                next_boundary = (
                    (current_time_ms // min_interval_ms) + 1
                ) * min_interval_ms
                sleep_seconds = (next_boundary - current_time_ms) * 0.001

                # Sleep until next boundary if needed
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)

                # Close ticker klines if enabled
                if tickers_enabled:
                    await self._close_tickers_klines(next_boundary)

                # Additional waiter delay for trades mode
                if waiter_enabled:
                    await asyncio.sleep(waiter_latency_ms / 1000)

                # Increment tick counter
                self._timer_ticks_count += 1

                # Close appropriate klines based on mode
                await self._close_klines(next_boundary)

        except asyncio.CancelledError:
            raise

    async def _close_klines(self, boundary_ms: int) -> None:
        """Close OHLC buckets and publish ready klines."""
        # Get references for faster access
        trades_buckets = self._trades_buckets
        trades_first_skipped = self._trades_first_kline_skipped
        trades_last_close = self._trades_last_close
        intervals_sorted = self._intervals_sorted

        closed_klines: list[dict[str, Any]] = []
        intervals_hit: set[int] = set()

        for symbol, intervals in trades_buckets.items():
            # Get or create tracking dicts for this symbol
            first_skipped_flags = trades_first_skipped.setdefault(symbol, {})
            last_close_values = trades_last_close.setdefault(symbol, {})

            for interval_ms in intervals_sorted:
                interval_buckets = intervals.get(interval_ms)

                # No buckets for this interval - create empty kline if possible
                if not interval_buckets:
                    last_close = last_close_values.get(interval_ms)
                    if last_close is not None and boundary_ms % interval_ms == 0:
                        closed_klines.append(
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
                        intervals_hit.add(interval_ms)
                    continue

                # Find buckets ready to close (past boundary)
                buckets_to_close = [
                    bucket_start
                    for bucket_start, bucket in interval_buckets.items()
                    if bucket["end"] <= boundary_ms
                ]
                if buckets_to_close:
                    intervals_hit.add(interval_ms)

                # Process each bucket that needs closing
                for bucket_start in sorted(buckets_to_close):
                    bucket = interval_buckets.pop(bucket_start)
                    next_bucket_start = bucket_start + interval_ms
                    next_bucket = interval_buckets.get(next_bucket_start)

                    bucket_close_price = bucket["c"]

                    # Handle first kline for this interval (skip it)
                    if not first_skipped_flags.get(interval_ms, False):
                        first_skipped_flags[interval_ms] = True
                        last_close_values[interval_ms] = bucket_close_price

                        # Ensure next bucket exists with proper open price
                        if next_bucket is None:
                            interval_buckets[next_bucket_start] = {
                                "start": next_bucket_start,
                                "end": next_bucket_start + interval_ms,
                                "o": bucket_close_price,
                                "h": bucket_close_price,
                                "l": bucket_close_price,
                                "c": bucket_close_price,
                                "v": 0.0,
                                "n": 0,
                            }
                        else:
                            next_bucket["o"] = bucket_close_price
                        continue

                    # Regular kline processing - ensure next bucket exists
                    if next_bucket is None:
                        interval_buckets[next_bucket_start] = {
                            "start": next_bucket_start,
                            "end": next_bucket_start + interval_ms,
                            "o": bucket_close_price,
                            "h": bucket_close_price,
                            "l": bucket_close_price,
                            "c": bucket_close_price,
                            "v": 0.0,
                            "n": 0,
                        }
                    else:
                        next_bucket["o"] = bucket_close_price

                    # Update last close for future empty klines
                    last_close_values[interval_ms] = bucket_close_price

                    # Create kline output
                    closed_klines.append(
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

        # Publish closed klines if any
        if closed_klines:
            total_symbols = self._klines_total_symbols_count
            channel_name = self._channel
            logger.info(
                "Closing %s/%s klines at boundary %s in channel %s; "
                "intervals closed: %s",
                len(closed_klines),
                total_symbols * len(intervals_hit),
                boundary_ms,
                channel_name,
                intervals_hit,
            )
            await self._broadcaster.consume(channel_name, "klines", closed_klines)
            await self._storage.process_klines(channel_name, closed_klines)

    async def _close_tickers_klines(self, boundary_ms: int) -> None:
        """Close ticker OHLC buckets and publish ready ticker klines."""
        # Get references for faster access
        tickers_buckets = self._tickers_buckets
        tickers_first_skipped = self._tickers_first_kline_skipped
        tickers_last_close = self._tickers_last_close
        tickers_last_boundary = self._tickers_last_boundary
        intervals_sorted = self._intervals_sorted

        closed_ticker_klines: list[dict[str, Any]] = []
        intervals_hit: set[int] = set()

        for symbol, intervals in tickers_buckets.items():
            # Get or create tracking dicts for this symbol
            first_skipped_flags = tickers_first_skipped.setdefault(symbol, {})
            last_close_values = tickers_last_close.setdefault(symbol, {})

            for interval_ms in intervals_sorted:
                # Update boundary tracking
                tickers_last_boundary[interval_ms] = boundary_ms

                interval_buckets = intervals.get(interval_ms, {})

                # No buckets for this interval - create empty kline if possible
                if not interval_buckets:
                    last_close_data = last_close_values.get(interval_ms)
                    if last_close_data is not None and boundary_ms % interval_ms == 0:
                        empty_kline: dict[str, Any] = {
                            "symbol": symbol,
                            "interval": interval_ms,
                            "timestamp": boundary_ms - interval_ms,
                            "n": 0,
                        }
                        # Add OHLC data for each field using last close values
                        for field_name, field_value in last_close_data.items():
                            empty_kline[field_name] = {
                                "o": field_value,
                                "h": field_value,
                                "l": field_value,
                                "c": field_value,
                            }
                        closed_ticker_klines.append(empty_kline)
                        intervals_hit.add(interval_ms)
                    continue

                # Find buckets ready to close (past boundary)
                buckets_to_close = [
                    bucket_start
                    for bucket_start, bucket in interval_buckets.items()
                    if bucket["end"] <= boundary_ms
                ]

                if buckets_to_close:
                    intervals_hit.add(interval_ms)

                # Process each bucket that needs closing
                for bucket_start in sorted(buckets_to_close):
                    bucket = interval_buckets.pop(bucket_start)
                    processed_bucket = self._process_single_ticker_bucket(
                        symbol,
                        interval_ms,
                        bucket_start,
                        bucket,
                        interval_buckets,
                        first_skipped_flags,
                        last_close_values,
                    )
                    if processed_bucket:
                        closed_ticker_klines.append(processed_bucket)

        # Publish closed ticker klines if any
        if closed_ticker_klines:
            total_symbols = self._klines_total_symbols_count
            channel_name = self._channel
            logger.info(
                "Closing %s/%s ticker klines at boundary %s in channel %s; "
                "intervals closed: %s",
                len(closed_ticker_klines),
                total_symbols * len(intervals_hit),
                boundary_ms,
                channel_name,
                intervals_hit,
            )
            await self._broadcaster.consume(
                channel_name, "tickers-klines", closed_ticker_klines
            )
            await self._storage.process_tickers_klines(
                channel_name, closed_ticker_klines
            )

    def _process_single_ticker_bucket(
        self,
        ticker_symbol: str,
        interval_ms: int,
        bucket_start: int,
        bucket: dict[str, Any],
        interval_buckets: dict[int, dict[str, Any]],
        first_kline_skipped: dict[int, bool],
        last_close_per_interval: dict[int, dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Process a single ticker bucket for closing."""
        numeric_fields = self._tickers_numeric_fields

        # Handle first kline: skip and initialize last_close
        if self._handle_first_ticker_kline(
            interval_ms,
            bucket,
            first_kline_skipped,
            last_close_per_interval,
            numeric_fields,
            ticker_symbol,
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
            ticker_symbol,
            bucket_start,
            interval_buckets,
        )

        # Create and return kline data
        return self._create_ticker_kline_data(
            ticker_symbol, interval_ms, bucket, numeric_fields
        )

    def _handle_first_ticker_kline(  # noqa: PLR0913
        self,
        interval_ms: int,
        bucket: dict[str, Any],
        first_kline_skipped: dict[int, bool],
        last_close_per_interval: dict[int, dict[str, Any]],
        numeric_fields: list[str],
        ticker_symbol: str,
        bucket_start: int,
        interval_buckets: dict[int, dict[str, Any]],
    ) -> bool:
        """Handle first kline for ticker interval. Returns True if skipped."""
        if first_kline_skipped.get(interval_ms, False):
            return False

        # Mark first kline as skipped
        first_kline_skipped[interval_ms] = True

        # Initialize last close values for this interval
        interval_last_close = last_close_per_interval.setdefault(interval_ms, {})
        interval_last_close.update(
            {field: bucket[field]["c"] for field in numeric_fields if field in bucket}
        )

        # Log missing fields
        for field_name in numeric_fields:
            if field_name not in bucket:
                logger.warning(
                    "Field %s missing in first ticker bucket for symbol=%s interval=%s",
                    field_name,
                    ticker_symbol,
                    interval_ms,
                )

        # Set next bucket open prices
        next_bucket_start = bucket_start + interval_ms
        next_bucket = interval_buckets.get(next_bucket_start)
        if next_bucket:
            for field_name in numeric_fields:
                if field_name in bucket and field_name in next_bucket:
                    next_bucket[field_name]["o"] = bucket[field_name]["c"]

        return True

    def _update_ticker_bucket_state(
        self,
        interval_ms: int,
        bucket: dict[str, Any],
        last_close_per_interval: dict[int, dict[str, Any]],
        numeric_fields: list[str],
        ticker_symbol: str,
        bucket_start: int,
        interval_buckets: dict[int, dict[str, Any]],
    ) -> None:
        """Update next bucket open prices and last close data."""
        # Set next bucket open prices
        next_bucket_start = bucket_start + interval_ms
        next_bucket = interval_buckets.get(next_bucket_start)
        if next_bucket:
            for field_name in numeric_fields:
                if field_name in bucket and field_name in next_bucket:
                    next_bucket[field_name]["o"] = bucket[field_name]["c"]

        # Update last close values for this interval
        interval_last_close = last_close_per_interval.setdefault(interval_ms, {})
        interval_last_close.update(
            {field: bucket[field]["c"] for field in numeric_fields if field in bucket}
        )

        # Log missing fields
        for field_name in numeric_fields:
            if field_name not in bucket:
                logger.warning(
                    "Field %s missing in ticker bucket for symbol=%s interval=%s",
                    field_name,
                    ticker_symbol,
                    interval_ms,
                )

    def _create_ticker_kline_data(
        self,
        ticker_symbol: str,
        interval_ms: int,
        bucket: dict[str, Any],
        numeric_fields: list[str],
    ) -> dict[str, Any]:
        """Create kline data dict from ticker bucket."""
        # Create base kline structure
        kline_data = {
            "symbol": ticker_symbol,
            "interval": interval_ms,
            "timestamp": bucket["start"],
            "n": bucket["n"],
        }

        # Add OHLC data for each numeric field present in bucket
        for field_name in numeric_fields:
            if field_name in bucket:
                kline_data[field_name] = dict(bucket[field_name])

        return kline_data

    async def _wait_klines(self, boundary_ms: int) -> None:
        """Wait for all closed klines, then publish."""
        # Get references for faster access
        klines_last_closed = self._klines_last_closed
        intervals_sorted = self._intervals_sorted
        total_symbols = self._klines_total_symbols_count
        channel_name = self._channel

        collected_klines: list[dict[str, Any]] = []

        for interval_ms in intervals_sorted:
            # Skip intervals that don't align with boundary
            if boundary_ms % interval_ms != 0:
                continue

            start_boundary = boundary_ms - interval_ms
            wait_time = 0.0

            # Wait for all symbols to be available
            while True:
                interval_closed_store = klines_last_closed.setdefault(interval_ms, {})
                available_klines = interval_closed_store.get(start_boundary, [])

                if len(available_klines) >= total_symbols:
                    break

                if wait_time >= KLINE_MAX_WAIT_TIME:
                    logger.warning(
                        "_wait_klines timeout: Received %s/%s symbols for interval %s "
                        "at start=%s; continuing with available.",
                        len(available_klines),
                        total_symbols,
                        interval_ms,
                        start_boundary,
                    )
                    break

                await asyncio.sleep(KLINE_SLEEP_TIME)
                wait_time += KLINE_SLEEP_TIME

            # Collect available klines for this interval
            interval_klines = klines_last_closed[interval_ms].pop(start_boundary, [])
            if interval_klines:
                collected_klines.extend(interval_klines)

        # Publish collected klines if any
        if collected_klines:
            logger.info(
                "Collect %s closed klines at boundary %s in channel %s",
                len(collected_klines),
                boundary_ms,
                channel_name,
            )
            await self._broadcaster.consume(channel_name, "klines", collected_klines)
            await self._storage.process_klines(channel_name, collected_klines)
