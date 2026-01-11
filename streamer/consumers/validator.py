"""Validator consumer for comparing local klines to Bybit API in real-time."""

import asyncio
import logging
import math
from typing import Any, Dict, List, Optional, Sequence

import aiohttp

from streamer.consumers.base import BaseConsumer
from streamer.settings import settings
from streamer.storage import Storage
from streamer.types import Channel, DataType, Interval

logger = logging.getLogger(__name__)

BYBIT_KLINE_ENDPOINT = "https://api.bybit.com/v5/market/kline"
LIMIT = 1  # only fetch 1 kline
CONCURRENCY = 3  # small concurrency for cost; adjust if needed


class ValidatorConsumer(BaseConsumer):
    """Simple validator: periodically compares output klines to Bybit API."""

    def __init__(self, storage: Storage, name: str = "validator") -> None:
        """Initialize ValidatorConsumer."""
        super().__init__(storage, name)
        self._is_running = False
        self._session: Optional[aiohttp.ClientSession] = None

    def validate(self) -> None:
        """Validate consumer settings (no-op)."""

    async def setup(self) -> None:
        """Set up the validator consumer."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        self.logger.info("ValidatorConsumer setup complete")

    async def start(self) -> None:
        """Start the validator consumer."""
        self._is_running = True
        self.logger.info("ValidatorConsumer started")

    async def stop(self) -> None:
        """Stop the validator consumer."""
        self._is_running = False
        if self._session:
            await self._session.close()
            self._session = None
        self.logger.info("ValidatorConsumer stopped")

    async def consume(
        self, channel: Channel, data_type: DataType, data: List[Dict[str, Any]]
    ) -> None:
        """
        For every local kline, compare basic fields to Bybit API's result.

        Only run if active, on kline data, and not in klines_mode.
        """
        if not (
            self._is_running
            and data
            and data_type == "klines"
            and not settings.klines_mode
        ):
            return

        session = self._session
        if session is None:
            self.logger.warning("ValidatorConsumer: No HTTP session available")
            return

        semaphore = asyncio.Semaphore(CONCURRENCY)
        tasks = [
            asyncio.create_task(
                self._validate_single_candle(channel, item, session, semaphore)
            )
            for item in data
        ]
        if not tasks:
            return

        results = await asyncio.gather(*tasks)

        mismatches = [diff for diff in results if diff is not None]
        checked = len(tasks)
        n_mismatches = len(mismatches)
        percent = (n_mismatches / checked * 100) if checked else 0.0

        if mismatches:
            for mismatch in mismatches:
                # line break for Ruff E501 compliance
                msg = (
                    f"[VALIDATOR] {mismatch['symbol']} {mismatch['interval']} "
                    f"{mismatch['timestamp']} "
                    + ", ".join(
                        f"{d['field']}: {d['local']}!={d['bybit']}"
                        for d in mismatch["diffs"]
                    )
                )
                self.logger.warning(msg)
        self.logger.info(
            f"[VALIDATOR SUMMARY] checked={checked} mismatches={n_mismatches} "
            f"({percent:.2f}%)"
        )

    async def _validate_single_candle(
        self,
        channel: Channel,
        local: Dict[str, Any],
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
    ) -> Optional[Dict[str, Any]]:
        """Fetch Bybit's authoritative kline for this candle; compare main fields."""
        async with sem:
            symbol = local.get("symbol")
            interval_ms = local.get("interval")
            timestamp = local.get("timestamp")
            if not (symbol and interval_ms and timestamp):
                return None

            remote = await self._fetch_bybit_kline(
                channel, symbol, interval_ms, timestamp, session
            )
            if remote is None:
                return None

            bybit_row = self._map_bybit_kline(remote)

            diffs = self._compare_kline_fields(local, bybit_row)
            if diffs:
                return {
                    "symbol": symbol,
                    "interval": interval_ms,
                    "timestamp": timestamp,
                    "diffs": diffs,
                }
            return None

    async def _fetch_bybit_kline(
        self,
        channel: Channel,
        symbol: Any,
        interval_ms: Any,
        timestamp: Any,
        session: aiohttp.ClientSession,
    ) -> Optional[list[int | float]]:
        try:
            interval = Interval(interval_ms)
            interval_str = interval.to_bybit()
            params = {
                "category": str(channel),
                "symbol": str(symbol),
                "interval": interval_str,
                "start": str(timestamp),
                "limit": str(LIMIT),
            }
            async with session.get(BYBIT_KLINE_ENDPOINT, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                result = data["result"]
                klines: list[list[int | float]] | None = result.get("list", [])
                if not klines or data.get("retCode") != 0:
                    return None
                return klines[0]
        except Exception as exc:
            self.logger.debug(f"Kline fetch error: {exc}")
            return None

    def _map_bybit_kline(self, remote: list[int | float]) -> Dict[str, Optional[Any]]:
        fields: List[str] = [
            "start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "end",
        ]
        return {
            fields[i]: remote[i] if i < len(remote) else None
            for i in range(len(fields))
        }

    def _compare_kline_fields(
        self, local: Dict[str, Any], bybit_row: Dict[str, Optional[Any]]
    ) -> List[Dict[str, Any]]:
        compare_keys: Sequence[str] = ("open", "high", "low", "close", "volume")
        diffs: List[Dict[str, Any]] = []
        for key in compare_keys:
            lv = local.get(key)
            bv = bybit_row.get(key)
            try:
                if lv is None or bv is None:
                    continue
                if key == "volume":
                    if not math.isclose(float(lv), float(bv), abs_tol=1e-6):
                        diffs.append({"field": key, "local": lv, "bybit": bv})
                elif float(lv) != float(bv):
                    diffs.append({"field": key, "local": lv, "bybit": bv})
            except Exception as exc:
                self.logger.debug(
                    f"Comparison error: field={key} lv={lv} bv={bv} exc={exc}"
                )
                continue
        return diffs
