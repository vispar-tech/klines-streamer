# Klines Streamer

A multi-exchange Python service that streams real-time market data from various cryptocurrency exchanges via WebSocket APIs. Due to Python's GIL limitations, each exchange runs in its own Docker container for optimal performance.

**Key features:**

-   Multi-exchange support: configurable exchange connectors
-   Real-time streaming: trades, klines, tickers, prices
-   Containerized architecture: each exchange in separate container
-   Extensible consumers: Redis, WebSocket, console, file storage, or your own
-   `.env` config with type-safe validation per exchange
-   Asyncio architecture with structured logging

---

## Quick Start

**1. Clone & Install:**

```bash
git clone https://github.com/vispar-tech/klines-streamer.git
cd klines-streamer
poetry install
cp .env.example .env
```

**2. Configure exchange:**

Each container supports only one exchange due to Python GIL limitations. Create separate `.env` files for each exchange:

```bash
# For Bybit streamer
cp .env.example .env.bybit
# Edit .env.bybit with Bybit-specific settings

# For Binance streamer
cp .env.example .env.binance
# Edit .env.binance with Binance-specific settings
```

**3. Run locally:**

For development, run single exchange locally:

```bash
# Run Bybit streamer
STREAMER_EXCHANGE=bybit poetry run python -m streamer

# Run Binance streamer
STREAMER_EXCHANGE=binance poetry run python -m streamer
```

**Or with Docker Compose (recommended):**

Each exchange runs in its own dedicated container:

```bash
# Run Bybit streamer
docker-compose --env-file .env.bybit up --build streamer-bybit

# Run Binance streamer
docker-compose --env-file .env.binance up --build streamer-binance

# Run multiple exchanges simultaneously
docker-compose --env-file .env.bybit up --build streamer-bybit &
docker-compose --env-file .env.binance up --build streamer-binance &
```

---

## .env Configuration

Create separate `.env` files for each exchange (e.g., `.env.bybit`, `.env.binance`). Each container loads only one exchange configuration:

### Exchange-Specific Settings

```
# Exchange Selection (required for each container)
STREAMER_EXCHANGE=bybit  # or 'binance', 'okx', etc.

# Exchange Symbols Configuration
STREAMER_EXCHANGE_SYMBOLS=BTCUSDT,ETHUSDT
STREAMER_EXCHANGE_LOAD_ALL_SYMBOLS=false
STREAMER_EXCHANGE_SYMBOLS_LIMIT=
STREAMER_EXCHANGE_SOCKET_POOL_SIZE=100

# Kline Intervals
STREAMER_KLINE_INTERVALS=1m,5m,1h,4h,1D
STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true
```

### Global Settings (shared across all containers)

```
# Streaming Configuration
STREAMER_ENABLE_KLINES_STREAM=true
STREAMER_ENABLE_PRICE_STREAM=false
STREAMER_ENABLE_TICKER_STREAM=false
STREAMER_ENABLE_TRADES_STREAM=false
STREAMER_ENABLE_SPOT_STREAM=false

# Consumer Configuration
STREAMER_ENABLED_CONSUMERS=redis,websocket,console,file

# Redis Configuration (shared across exchanges)
STREAMER_REDIS_HOST=redis-host
STREAMER_REDIS_PORT=6379
STREAMER_REDIS_USER=
STREAMER_REDIS_PASSWORD=
STREAMER_REDIS_BASE=0
STREAMER_REDIS_MAIN_KEY=market-data

# WebSocket Server Configuration
STREAMER_WEBSOCKET_HOST=0.0.0.0
STREAMER_WEBSOCKET_PORT=9500
STREAMER_WEBSOCKET_PATH=/

# WebSocket Authentication (required for WSS output)
STREAMER_WSS_AUTH_KEY=your_secret_key
STREAMER_WSS_AUTH_USER=admin

# Storage Configuration
STREAMER_STORAGE_ENABLED=false
STREAMER_STORAGE_BUFFER_FLUSH_SIZE=100
STREAMER_STORAGE_BUFFER_FLUSH_INTERVAL=1.0

# Logging Configuration
STREAMER_LOG_LEVEL=INFO
STREAMER_LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s
STREAMER_LOG_FILE=

# Traefik Configuration (if using traefik docker-compose)
STREAMER_HOST=your-domain.com
```

> **Important:** Each exchange container must have its own `STREAMER_EXCHANGE` setting and dedicated WebSocket port to avoid conflicts.

---

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

Aggregated candlestick (OHLCV) data (exchange-agnostic format):

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

### Ticker Data

Full market ticker information:

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
    "exchange": "bybit",
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

### Trades Data

Raw trade information:

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
    "exchange": "bybit",
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

### Multi-Exchange Containerized Design

Due to Python's Global Interpreter Lock (GIL), each cryptocurrency exchange runs in its own dedicated Docker container:

- **Exchange Isolation**: Each container handles exactly one exchange to maximize performance
- **Independent Scaling**: Scale individual exchanges without affecting others
- **Resource Management**: Better CPU utilization and memory management per exchange
- **Fault Tolerance**: Exchange failures are isolated to their specific containers

### Broadcaster per Exchange

Each exchange container has its own `Broadcaster` component that:

-   Receives streaming data from the exchange's WebSocket API
-   Supports multiple data types: klines, tickers, prices, trades
-   Forwards data to all enabled consumers simultaneously using `asyncio.gather`
-   Logs broadcasting activity (with spam protection for frequent updates)
-   Handles consumer errors individually without affecting the exchange stream

### Storage

When `STREAMER_STORAGE_ENABLED=true`, the `Storage` component acts as a consumer that persists data to Redis:

-   **Connection**: Establishes Redis connection using `STREAMER_REDIS_URL`
-   **Data Processing**: Maintains local snapshots per symbol, applies deltas for efficient updates
-   **Key Structure**: Uses configurable `STREAMER_REDIS_MAIN_KEY` (default: "bybit-streamer") as prefix:
    -   `{main_key}:tickers:{channel}` - hash with full ticker snapshots per symbol
    -   `{main_key}:price:{channel}` - hash with current prices per symbol
    -   `{main_key}:last-closed:{channel}:{interval}` - klines data with TTL expiration
-   **Initialization**: Cleans existing keys on startup for consistent state

### File Consumer

The `FileConsumer` saves streaming data to organized file structure with automatic cleanup:

-   **Directory Structure**: Organizes files by date/time: `output/file_consumer/YYYY/MM/DD/HH/`
-   **File Naming**: Uses timestamp-based filenames: `YYYYMMDD_HHMMSS_mmmmmm_{channel}_{data_type}.jsonl`
-   **Data Format**: Saves data as JSON Lines with metadata (channel, data_type, timestamp)
-   **Automatic Cleanup**: Removes files older than 4 hours every 5 minutes to prevent disk space issues
-   **Concurrent Safe**: Each `consume()` call creates a new file, ensuring no conflicts

Example file structure (organized by exchange):

```
output/file_consumer/
├── bybit/
│   └── 2026/
│       └── 01/
│           └── 12/
│               └── 15/
│                   ├── 20260112_150000_118938_linear_klines.jsonl
│                   └── 20260112_150005_234567_spot_ticker.jsonl
├── binance/
│   └── 2026/
│       └── 01/
│           └── 12/
│               └── 15/
│                   ├── 20260112_150000_118938_spot_klines.jsonl
│                   └── 20260112_150005_234567_futures_ticker.jsonl
```

---

## Development

-   **Quality:** Ruff (lint/format), MyPy (types), Black (format)
-   **Multi-exchange development:**
     - Each exchange has its own container and configuration
     - Use separate `.env` files for different exchanges
     - Test exchanges independently during development
-   **Commands:**
     `make run` — start single exchange app
     `make check-all` — lint & type checks
     `pre-commit run --all-files` — run hooks

---

MIT License © Daniil Pavlovich
