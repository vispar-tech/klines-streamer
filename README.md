# Bybit Streamer

A Python service that streams real-time market data from Bybit WebSocket API including trades, klines, tickers, and prices. Supports multiple channels (linear/spot), aggregation modes, and extensible consumers.

**Key features:**

-   Real-time streaming: trades, klines, tickers, prices
-   Multi-channel support: linear perpetuals and spot markets
-   Configurable aggregation modes (trade-based or kline-based)
-   Extensible consumers: Redis, WebSocket, console, storage, or your own
-   `.env` config with type-safe validation
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
# Bybit Configuration
STREAMER_BYBIT_SYMBOLS=BTCUSDT,ETHUSDT
STREAMER_BYBIT_LOAD_ALL_SYMBOLS=false
STREAMER_BYBIT_SYMBOLS_LIMIT=
STREAMER_BYBIT_SOCKET_POOL_SIZE=100
STREAMER_KLINE_INTERVALS=1m,5m,1h,4h,1D
STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true
STREAMER_KLINES_MODE=false

# Streaming Configuration
STREAMER_ENABLE_KLINES_STREAM=true
STREAMER_ENABLE_PRICE_STREAM=false
STREAMER_ENABLE_TICKER_STREAM=false
STREAMER_ENABLE_TRADES_STREAM=false
STREAMER_ENABLE_SPOT_STREAM=false

# Redis Configuration
STREAMER_REDIS_HOST=
STREAMER_REDIS_PORT=
STREAMER_REDIS_USER=
STREAMER_REDIS_PASSWORD=
STREAMER_REDIS_BASE=
STREAMER_REDIS_MAIN_KEY=

# WebSocket Server Configuration
STREAMER_WEBSOCKET_HOST=
STREAMER_WEBSOCKET_PORT=9500
STREAMER_WEBSOCKET_PATH=

# WebSocket Authentication (required for WSS output)
STREAMER_WSS_AUTH_KEY=your_secret_key
STREAMER_WSS_AUTH_USER=

# Consumer Configuration
STREAMER_ENABLED_CONSUMERS=redis,websocket,console

# Storage Configuration
STREAMER_STORAGE_ENABLED=false
STREAMER_STORAGE_BUFFER_FLUSH_SIZE=100
STREAMER_STORAGE_BUFFER_FLUSH_INTERVAL=1.0

# Logging Configuration
STREAMER_LOG_LEVEL=INFO
STREAMER_LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s
STREAMER_LOG_FILE=

# If use traefik docker-compose and websocket
STREAMER_HOST=your-domain.com
```

> **WSS output:** You must set `STREAMER_WSS_AUTH_KEY` and `STREAMER_WSS_AUTH_USER` to restrict client access.

---

## Streaming Modes

### Aggregation Modes

-   **Trade-based aggregation** (`STREAMER_KLINES_MODE=false`): Builds klines from individual trades in real-time
    -   Enable waiter mode: `STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true`
    -   Tune latency (80-150ms): `STREAMER_AGGREGATOR_WAITER_LATENCY_MS=80`
    -   May have slight delays compared to official Bybit klines
-   **Klines mode** (`STREAMER_KLINES_MODE=true`): Uses Bybit's official kline stream, more stable, but with ~1s latency

### Data Streams

Control which data types to stream using these settings:

-   `STREAMER_ENABLE_KLINES_STREAM=true`: Aggregate and stream kline (OHLCV) data
-   `STREAMER_ENABLE_TRADES_STREAM=false`: Stream raw trade data
-   `STREAMER_ENABLE_TICKER_STREAM=false`: Stream ticker snapshots and deltas
-   `STREAMER_ENABLE_PRICE_STREAM=false`: Stream price updates only

### Channels

Supports both linear perpetuals (`linear`) and spot (`spot`) markets via WebSocket connections.

## Data Examples

### Klines Data

Aggregated candlestick (OHLCV) data:

```json
{
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

### Ticker Data

Full market ticker information:

```json
{
    "channel": "linear",
    "data_type": "ticker",
    "data": [
        {
            "symbol": "BTCUSDT",
            "tickDirection": "PlusTick",
            "price24hPcnt": -0.010668,
            "lastPrice": 92792.1,
            "prevPrice24h": 93792.7,
            "highPrice24h": 94749.9,
            "lowPrice24h": 92762.3,
            "prevPrice1h": 94206.7,
            "markPrice": 92792.1,
            "indexPrice": 92845.88,
            "openInterest": 51295.867,
            "openInterestValue": 4759851220.25,
            "turnover24h": 6112179211.7902,
            "volume24h": 65148.115,
            "fundingIntervalHour": 8.0,
            "fundingCap": 0.005,
            "nextFundingTime": 1767744000000,
            "fundingRate": -0.00006019,
            "bid1Price": 92792.0,
            "bid1Size": 5.203,
            "ask1Price": 92792.1,
            "ask1Size": 1.528,
            "preOpenPrice": "",
            "preQty": "",
            "curPreListingPhase": ""
        }
    ]
}
```

Spot ticker (simplified):

```json
{
    "channel": "spot",
    "data_type": "ticker",
    "data": [
        {
            "symbol": "BTCUSDT",
            "lastPrice": 92846.1,
            "highPrice24h": 94774.0,
            "lowPrice24h": 92815.5,
            "prevPrice24h": 93839.6,
            "volume24h": 10425.837779,
            "turnover24h": 978626382.2024397,
            "price24hPcnt": -0.0106,
            "usdIndexPrice": 92802.53128
        }
    ]
}
```

### Price Data

Simplified price-only updates:

```json
{
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

### Trades Data

Raw trade information:

```json
{
    "channel": "linear",
    "data_type": "trades",
    "data": [
        {
            "T": 1767716999877,
            "s": "BTCUSDT",
            "S": "Sell",
            "v": "0.100",
            "p": "92792.00",
            "L": "MinusTick",
            "i": "ffe924d3-eee1-5b19-af47-248825c37611",
            "BT": false,
            "RPI": false,
            "seq": 508178141815
        }
    ]
}
```

Spot trades (batch):

```json
{
    "channel": "spot",
    "data_type": "trades",
    "data": [
        {
            "i": "2290000000984259176",
            "T": 1767717000313,
            "p": "92846",
            "v": "0.00163",
            "S": "Sell",
            "seq": 95557774872,
            "s": "BTCUSDT",
            "BT": false,
            "RPI": false
        }
    ]
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

## Architecture

### Broadcaster

The `Broadcaster` component receives streaming data from multiple channels and distributes it to all registered consumers in parallel. It:

-   Receives data from WebSocket streams across channels (linear/spot)
-   Supports multiple data types: klines, tickers, prices, trades
-   Forwards data to all enabled consumers simultaneously using `asyncio.gather`
-   Logs broadcasting activity (with spam protection for frequent updates)
-   Handles consumer errors individually without affecting others

### Storage

When `STREAMER_STORAGE_ENABLED=true`, the `Storage` component acts as a consumer that persists data to Redis:

-   **Connection**: Establishes Redis connection using `STREAMER_REDIS_URL`
-   **Data Processing**: Maintains local snapshots per symbol, applies deltas for efficient updates
-   **Key Structure**: Uses configurable `STREAMER_REDIS_MAIN_KEY` (default: "bybit-streamer") as prefix:
    -   `{main_key}:tickers:{channel}` - hash with full ticker snapshots per symbol
    -   `{main_key}:price:{channel}` - hash with current prices per symbol
    -   `{main_key}:last-closed:{channel}:{interval}` - klines data with TTL expiration
-   **Initialization**: Cleans existing keys on startup for consistent state

---

## Development

-   **Quality:** Ruff (lint/format), MyPy (types), Black (format)
-   **Commands:**  
     `make run` — start app  
     `make check-all` — lint & type checks  
     `pre-commit run --all-files` — run hooks

---

MIT License © Daniil Pavlovich
