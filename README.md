# Klines Streamer

A multi-exchange Python service for streaming real-time crypto market data from major exchanges via WebSocket APIs. Each exchange runs in a dedicated Docker container for performance and separation.

**Highlights:**

- Multi-exchange support (add connectors as needed)
- Real-time streams: trades, klines, tickers, prices
- Each exchange is isolated in its own container
- Extensible consumers: Redis, WebSocket, console, file, or custom
- Per-exchange `.env` config with type validation
- Asyncio-based, with structured logging

---

## Quick Start

**1. Clone & install:**

```bash
git clone https://github.com/vispar-tech/klines-streamer.git
cd klines-streamer
poetry install
cp .env.example .env
```

**2. Configure an exchange:**

Each Docker container handles one exchange. Prepare `.env` files:

```bash
cp .env.example .env.bybit   # then edit for Bybit
cp .env.example .env.bingx # then edit for BingX
```

**3. Run locally:**

```bash
# Run one streamer at a time:
STREAMER_EXCHANGE=bybit poetry run python -m streamer
STREAMER_EXCHANGE=bingx poetry run python -m streamer
```

**Or with Docker Compose:**

```bash
docker-compose up --build streamer-bybit
docker-compose up --build streamer-bingx

# Or run both in background:
docker-compose up --build streamer-bybit &
docker-compose up --build streamer-bingx &

# To run the unified proxy (aggregates all exchanges)
docker-compose up --build streamer-proxy
```

---

## .env Configuration

**Per-exchange:** (e.g. `.env.bybit`, `.env.bingx`)

```
STREAMER_EXCHANGE=bybit  # bingx, etc.
STREAMER_EXCHANGE_SYMBOLS=BTCUSDT,ETHUSDT
STREAMER_EXCHANGE_LOAD_ALL_SYMBOLS=false
STREAMER_EXCHANGE_SYMBOLS_LIMIT=
STREAMER_EXCHANGE_SOCKET_POOL_SIZE=50
STREAMER_KLINE_INTERVALS=1m,5m,1h,4h,1D
STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true
```

**Global settings:** (in every `.env`)

```
STREAMER_ENABLE_KLINES_STREAM=true
STREAMER_ENABLE_PRICE_STREAM=false
STREAMER_ENABLE_TICKER_STREAM=false
STREAMER_ENABLE_SPOT_STREAM=false
STREAMER_ENABLED_CONSUMERS=redis,websocket,console,file

STREAMER_REDIS_HOST=redis-host
STREAMER_REDIS_PORT=6379
STREAMER_REDIS_MAIN_KEY=market-data

STREAMER_WEBSOCKET_HOST=0.0.0.0
STREAMER_WEBSOCKET_PORT=9500
STREAMER_WEBSOCKET_PATH=/

STREAMER_WSS_AUTH_KEY=your_secret_key
STREAMER_WSS_AUTH_USER=admin

STREAMER_LOG_LEVEL=INFO
STREAMER_LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s
STREAMER_LOG_FILE=
```

> **Note:** Each container must use a unique `STREAMER_EXCHANGE` and WebSocket port.

---

## Streams & Channels

Enable streams in `.env`:

- `STREAMER_ENABLE_KLINES_STREAM=true` # OHLCV candlesticks
- `STREAMER_ENABLE_TICKER_STREAM=false` # Ticker data
- `STREAMER_ENABLE_PRICE_STREAM=false` # Price-only
  Supports both linear perpetuals and spot.

---

## Data Examples

<details>
<summary><strong>Klines</strong></summary>

```json
{
    "exchange": "bybit",
    "channel": "spot",
    "data_type": "klines",
    "data": [
        {
            "symbol": "BTCUSDT",
            "interval": 60000,
            "timestamp": 1767716940000,
            "open": 92967.8,
            "high": 92970.8,
            "low": 92815.5,
            "close": 92846.1,
            "volume": 33.40428499999953,
            "trade_count": 3664
        }
    ]
}
```

</details>

<details>
<summary><strong>Ticker</strong></summary>

```json
{
    "exchange": "bybit",
    "channel": "linear",
    "data_type": "ticker",
    "data": [
        {
            "symbol": "BTCUSDT",
            "tickDirection": "PlusTick",
            "price24hPcnt": -0.010668,
            "lastPrice": 92792.1
            // ...other fields
        }
    ]
}
```

</details>

<details>
<summary><strong>Price</strong></summary>

```json
{
    "exchange": "bybit",
    "channel": "linear",
    "data_type": "price",
    "data": [
        {
            "symbol": "BTCUSDT",
            "price": 92792.1
        }
    ]
}
```

</details>

<details>
<summary><strong>Trades</strong></summary>

```json
{
    "exchange": "bybit",
    "channel": "linear",
    "data_type": "trades",
    "data": [
        {
            "T": 1767716999877,
            "s": "BTCUSDT",
            "S": "Sell",
            "v": "0.100",
            "p": "92792.00"
            // ...other fields
        }
    ]
}
```

</details>

---

## Custom Consumers

To implement your own consumer:

```python
from typing import Any, Dict, List

from streamer.consumers.base import BaseConsumer
from streamer.storage import Storage
from streamer.types import Channel, DataType

class MyConsumer(BaseConsumer):
    def __init__(self, storage: Storage, name: str = "my") -> None:
        super().__init__(storage, name)

    def validate(self) -> None:
        """Validate consumer settings."""
        pass

    async def setup(self) -> None:
        """Set up consumer."""
        pass

    async def start(self) -> None:
        """Start consumer."""
        self._is_running = True

    async def consume(
        self, channel: Channel, data_type: DataType, data: List[Dict[str, Any]]
    ) -> None:
        """Consume and process data."""
        pass

    async def stop(self) -> None:
        """Stop consumer."""
        self._is_running = False

from streamer.consumers import ConsumerManager
ConsumerManager.register_consumer("my", MyConsumer)
```

Add `"my"` to `STREAMER_ENABLED_CONSUMERS` in your `.env`.

---

## Design Overview

- **Each exchange**: One container—scale or restart independently.
- **Broadcaster**: Forwards WebSocket data to all consumers in parallel (async).
- **Consumers**: Redis/file/console/WebSocket, can run in any combo per container.
- **FileConsumer**: Writes JSONL to disk, organizes by date, cleans up old files.
- **Proxy**: Aggregates from all containers to a single endpoint for end users.
- **Storage**: Redis consumer uses per-symbol hashes for efficient access.

Example file output (per-exchange):

```
output/file_consumer/
├── bybit/2026/01/12/15/20260112_150000_118938_linear_klines.jsonl
├── bingx/2026/01/12/15/20260112_150000_118938_spot_klines.jsonl
```

---

## Development

- **Code quality:** `ruff`, `mypy`, `black` (`make check-all`)
- **Multi-exchange:** Separate containers and `.env` per exchange; test separately
- **Handy commands:**
    - `make run` — run one exchange app
    - `make check-all` — format, lint & type check
    - `pre-commit run --all-files` — all pre-commit hooks

---

MIT License © Daniil Pavlovich
