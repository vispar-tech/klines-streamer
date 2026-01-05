# Bybit Klines Streamer

A Python service that aggregates trades from Bybit WebSocket API to candlestick (kline/OHLCV) data at configurable intervals. Source logic is for Bybit only; outputs are extendable via custom consumers.

**Key features:**

-   Realtime kline (OHLCV) aggregation from trades
-   Support for multiple candle intervals
-   Extensible consumers: Redis, WebSocket, console, or your own
-   `.env` config (see below), type-safe validation
-   Asyncio architecture with structured logging

---

## Quick Start

**1. Clone & Install:**

```bash
git clone https://github.com/vispar-tech/bybit-klines-streamer.git
cd bybit-klines-streamer
poetry install
cp .env.example .env
```

**2. Run locally:**

```bash
poetry run python -m streamer
```

**Or with Docker Compose:**

```bash
docker-compose up --build
```

---

## .env Configuration

Edit `.env` for consumers, connection, and intervals:

```
STREAMER_ENABLED_CONSUMERS=console,redis,websocket
STREAMER_REDIS_URL=redis://localhost:6379/0
STREAMER_WEBSOCKET_PORT=9500
STREAMER_WSS_AUTH_KEY=your_secret_key
STREAMER_BYBIT_SYMBOLS=BTCUSDT,ETHUSDT
STREAMER_KLINE_INTERVALS=1m,5m,1h
STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true
STREAMER_AGGREGATOR_WAITER_LATENCY_MS=80
STREAMER_KLINES_MODE=false
STREAMER_STORAGE_ENABLED=false
STREAMER_STORAGE_BUFFER_FLUSH_SIZE=100
STREAMER_STORAGE_BUFFER_FLUSH_INTERVAL=1.0
STREAMER_REDIS_URL=redis://localhost:6379/0
STREAMER_LOG_LEVEL=INFO
```

> **WSS output:** You must set `STREAMER_WSS_AUTH_KEY` and `STREAMER_WSS_AUTH_USER` to restrict client access.

---

## Main Modes

-   **Trade-based aggregation** (`STREAMER_KLINES_MODE=false`): Realtime; candles may not match Bybit's official data due to trade delivery delays.
    -   Enable waiter mode: `STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true`
    -   Tune latency (80-150ms): `STREAMER_AGGREGATOR_WAITER_LATENCY_MS=80`
    -   Increase socket pool (50-100): `STREAMER_BYBIT_SOCKET_POOL_SIZE=50`
-   **Klines mode** (`STREAMER_KLINES_MODE=true`): Uses Bybit's own kline stream, typically more stable, ~1s latency, not all intervals supported.

**Example trade-based kline:**

```json
{
    "symbol": "BTCUSDT",
    "interval": 60000,
    "timestamp": 1767623520000,
    "open": 53220.0,
    "high": 53280.0,
    "low": 53130.1,
    "close": 53252.3,
    "volume": 3.83,
    "trade_count": 7
}
```

---

## Extend: Custom Consumer

```python
from streamer.consumers.base import BaseConsumer
class MyConsumer(BaseConsumer):
    def __init__(self, name="my"): super().__init__(name)
    def validate(self): pass
    async def setup(self): pass
    async def start(self): self._is_running = True
    async def consume(self, data): pass
    async def stop(self): self._is_running = False

from streamer.consumers import ConsumerManager
ConsumerManager.register_consumer("my", MyConsumer)
```

Add `"my"` to `STREAMER_ENABLED_CONSUMERS` in `.env`.

---

## Storage

When `STREAMER_STORAGE_ENABLED=true`, tickers data is stored in Redis as hashes: `bybit:tickers` (full snapshots) and `bybit:mark-price` (mark prices per symbol). Requires `STREAMER_REDIS_URL`.

---

## Development

-   **Quality:** Ruff (lint/format), MyPy (types), Black (format)
-   **Commands:**  
     `make run` — start app  
     `make check-all` — lint & type checks  
     `pre-commit run --all-files` — run hooks

---

MIT License © Daniil Pavlovich
