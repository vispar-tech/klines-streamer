"""Validator consumer for comparing local klines to Bybit API in real-time."""

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, DefaultDict, Dict, List, Optional, Sequence

import aiohttp

from streamer.consumers.base import BaseConsumer
from streamer.settings import settings

if TYPE_CHECKING:
    from collections.abc import Mapping

BYBIT_KLINE_ENDPOINT = "https://api.bybit.com/v5/market/kline"
CONCURRENCY = 5  # Number of parallel validation tasks per interval


class ValidatorConsumer(BaseConsumer):
    """Validate incoming kline data by comparing with Bybit API."""

    def __init__(self, name: str = "validator") -> None:
        """Initialize the ValidatorConsumer."""
        super().__init__(name)
        self._is_running = False
        self._session: Optional[aiohttp.ClientSession] = None

    def validate(self) -> None:
        """No custom config validation logic yet."""

    async def setup(self) -> None:
        """Set up the aiohttp ClientSession."""
        self.logger.info("Setting up validator consumer")
        if self._session is None:
            self._session = aiohttp.ClientSession()

    async def start(self) -> None:
        """Start the validator consumer."""
        self.logger.info("Starting validator consumer")
        self._is_running = True

    async def stop(self) -> None:
        """Stop the validator consumer and close the session."""
        self.logger.info("Stopping validator consumer")
        self._is_running = False
        if self._session:
            await self._session.close()
            self._session = None

    async def consume(self, data: List[Dict[str, Any]]) -> None:
        """
        Validate klines by comparing each to Bybit's reference.

        data: list of dicts, each representing a candle.
        """
        if not self._is_running or not data or settings.klines_mode:
            return

        # Group by interval (milliseconds)
        grouped: DefaultDict[int, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            grouped[int(item["interval"])].append(item)

        tasks: List["asyncio.Future[Any]"] = []
        for interval_ms, candles in grouped.items():
            # For each interval group, validate all candles concurrently (with limit)
            tasks.append(
                asyncio.create_task(self._validate_group(candles, interval_ms))
            )

        if tasks:
            await asyncio.gather(*tasks)

    async def _validate_group(
        self, candles: List[Dict[str, Any]], interval_ms: int
    ) -> None:
        sem = asyncio.Semaphore(CONCURRENCY)
        # Validate all candles, result is list of diffs
        validator_tasks = [
            asyncio.create_task(self._validate_candle(item, interval_ms, sem))
            for item in candles
        ]
        # Gather as they finish, log mismatches
        for fut in asyncio.as_completed(validator_tasks):
            res: Optional[Dict[str, Any]] = await fut
            if res:  # res is a diff dict if mismatch, else None
                self._report_diff(res)

    async def _validate_candle(
        self, local: Dict[str, Any], interval_ms: int, sem: asyncio.Semaphore
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch Bybit kline for this candle and compare.

        Return a dict describing the diff if mismatch, or None.
        """
        async with sem:
            session = self._session
            if session is None:
                return None

            symbol = local.get("symbol")
            bybit_interval = str(round(interval_ms / 60_000))
            timestamp = local.get("timestamp")
            if not (symbol and bybit_interval and timestamp):
                return None
            bybit_fields = [
                "start",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "end",
            ]
            remote_klines = await self._fetch_bybit_kline(
                session, symbol, bybit_interval, start=timestamp
            )
            if (
                not remote_klines
                or not isinstance(remote_klines, list)
                or not remote_klines
            ):
                return None

            remote_row = remote_klines[0]
            bybit = self._candle_dict_from_bybit_row(remote_row, bybit_fields)
            # Compare open/high/low/close fields (float)
            diffs: List[Dict[str, Any]] = []
            for k in ("open", "high", "low", "close"):
                local_v = local.get(k)
                bybit_v = bybit.get(k)
                if local_v is None or bybit_v is None:
                    continue
                try:
                    if float(local_v) != float(bybit_v):
                        diffs.append(
                            {
                                "field": k,
                                "local": local_v,
                                "bybit": bybit_v,
                                "diff": self._try_float_diff(local_v, bybit_v),
                                "percent_diff": self._try_percent_diff(
                                    local_v, bybit_v
                                ),
                            }
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Numeric comparison failed for {k}: {local_v}, {bybit_v} ({e})"
                    )
                    continue
            if diffs:
                return {
                    "symbol": symbol,
                    "interval": interval_ms,
                    "timestamp": timestamp,
                    "diffs": diffs,
                    "local": {
                        k: local.get(k)
                        for k in (
                            "timestamp",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "trade_count",
                        )
                    },
                    "bybit": {k: bybit.get(k) for k in bybit_fields},
                }
            return None

    async def _fetch_bybit_kline(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        start: int,
        end: Optional[int] = None,
        limit: int = 1,
    ) -> Optional[Sequence[Any]]:
        query_params: Mapping[str, str] = {
            "category": "linear",
            "symbol": str(symbol),
            "interval": str(interval),
            "start": str(start),
            "limit": str(limit),
        }
        if end is not None:
            query_params = {**query_params, "end": str(end)}
        try:
            async with session.get(BYBIT_KLINE_ENDPOINT, params=query_params) as resp:
                if resp.status != 200:
                    return None
                result = await resp.json()
                if not isinstance(result, dict) or result.get("retCode") != 0:
                    return None
                kline_list = result.get("result", {}).get("list")
                if not isinstance(kline_list, list):
                    return None
                return kline_list
        except Exception as e:
            self.logger.warning(f"Failed to fetch Bybit kline: {e}")
            return None

    def _candle_dict_from_bybit_row(
        self, row: Sequence[Any], fields: List[str]
    ) -> Dict[str, Any]:
        return {
            field: row[idx] if idx < len(row) else None
            for idx, field in enumerate(fields)
        }

    def _try_float_diff(self, a: Any, b: Any) -> Optional[float]:
        try:
            return float(a) - float(b)
        except (TypeError, ValueError) as e:
            self.logger.warning(f"Float diff failed: {a}, {b} ({e})")
            return None

    def _try_percent_diff(self, a: Any, b: Any) -> Optional[float]:
        try:
            af = float(a)
            bf = float(b)
            if bf == 0:
                return None
            return (af - bf) / abs(bf) * 100
        except (TypeError, ValueError, ZeroDivisionError) as e:
            self.logger.warning(f"Percent diff failed: {a}, {b} ({e})")
            return None

    def _report_diff(self, diff: Dict[str, Any]) -> None:
        # Compose a concise log message of diffs
        symbol = diff.get("symbol", "?")
        interval = diff.get("interval", "?")
        timestamp = diff.get("timestamp", "?")
        parts: List[str] = []
        for d in diff.get("diffs", []):
            field = d.get("field")
            local_v = d.get("local")
            bybit_v = d.get("bybit")
            float_diff = d.get("diff")
            percent_diff = d.get("percent_diff")
            if percent_diff is not None:
                try:
                    parts.append(
                        f"{field}: local={local_v}, bybit={bybit_v}, "
                        f"diff={float_diff}, "
                        f"percent_diff={percent_diff:.6f}%"
                    )
                except Exception:
                    parts.append(
                        f"{field}: local={local_v}, bybit={bybit_v}, "
                        f"diff={float_diff}, "
                        f"percent_diff={percent_diff}%"
                    )
            else:
                parts.append(
                    f"{field}: local={local_v}, bybit={bybit_v}, diff={float_diff}"
                )
        log_msg = (
            f"[VALIDATOR] {symbol} interval={interval} "
            f"timestamp={timestamp} :: mismatch: " + "; ".join(parts)
        )
        # Log warning if percent diff > 1 for open/close, else info
        is_critical = any(
            d.get("field") in ("open", "close")
            and d.get("percent_diff") is not None
            and abs(float(d["percent_diff"])) > 1.0
            for d in diff.get("diffs", [])
        )
        if is_critical:
            self.logger.warning(f"{log_msg} [CRITICAL!]")
        else:
            self.logger.info(log_msg)
