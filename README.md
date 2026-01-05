# Bybit Klines Streamer

A Python service that **aggregates trades from Bybit’s WebSocket API into candlestick (kline) data** at multiple configurable intervals. The exchange-layer logic is hardcoded for Bybit and this repository is **not designed for implementing other exchanges**. However, the system does support **pluggable consumers**—you can extend outputs, but not sources.

> **Note:** If you use WSS output, you must configure **authorization and an access key** for clients connecting to the WebSocket server (see below).

---

## Features

-   **Kline Aggregation:** Converts raw trades from Bybit into candlestick (kline) data (OHLCV)
-   **Multi-Interval Support:** Streams candles for multiple, configurable intervals
-   **Pluggable Consumers:** Extensible architecture (Redis, WebSocket, console, or your own)
-   **Config Validation:** Strict, consumer-specific settings validation
-   **Dynamic Symbols:** Load (and validate) all symbols from Bybit or provide a custom list
-   **Type Safety:** All config via Pydantic with env var support
-   **Async Architecture:** Fully asyncio-based for concurrent high performance
-   **Structured Logging:** Logs throughout app lifecycle
-   **Trade Latency Compensation:** Waiter mode for delayed trades from Bybit

---

## Usage

### Local Development

1. **Clone the repo:**

    ```bash
    git clone https://github.com/vispar-tech/bybit-klines-streamer.git
    cd bybit-klines-streamer
    ```

2. **Install dependencies:**

    ```bash
    poetry install
    ```

3. **Configure settings:**

    Copy and edit the example dotenv file:

    ```bash
    cp .env.example .env
    ```

### Docker Deployment

1. **Build and run with Docker Compose:**

    ```bash
    docker-compose up --build
    ```

2. **Run in background:**

    ```bash
    docker-compose up -d --build
    ```

3. **View logs:**

    ```bash
    docker-compose logs -f streamer
    ```

4. **Stop the service:**
    ```bash
    docker-compose down
    ```

### Docker Deployment with Traefik (Production)

1. **SSL via Traefik:**

    ```bash
    docker-compose -f docker-compose.traefik.yaml up -d --build
    ```

2. **Configure Traefik Environment:**

    - Make sure `STREAMER_HOST` and `STREAMER_WEBSOCKET_PORT` are set for proper Traefik routing.
    - _Traefik itself is not included; this assumes your own running Traefik and `traefiktointernet` network!_

---

### Configuration

Configure consumers and other settings via `.env` file:

```dotenv
# Consumers (comma-separated)
STREAMER_ENABLED_CONSUMERS=console,redis,websocket

# Redis (if enabled)
STREAMER_REDIS_URL=redis://localhost:6379/0
STREAMER_REDIS_CHANNEL=klines

# WebSocket (if enabled)
STREAMER_WEBSOCKET_HOST=localhost
STREAMER_WEBSOCKET_PORT=9500
STREAMER_WEBSOCKET_URL=wss://localhost:9500
STREAMER_WSS_AUTH_KEY=your_secret_key
STREAMER_WSS_AUTH_USER=your_username

# Symbol configuration
STREAMER_BYBIT_LOAD_ALL_SYMBOLS=false
STREAMER_BYBIT_SYMBOLS=BTCUSDT,ETHUSDT
STREAMER_BYBIT_SYMBOLS_LIMIT=
STREAMER_BYBIT_SOCKET_POOL_SIZE=5

# Kline intervals
STREAMER_KLINE_INTERVALS=1m,5m,1h

# Aggregator
STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true
STREAMER_AGGREGATOR_WAITER_LATENCY_MS=80
STREAMER_KLINES_MODE=false

# Logging
STREAMER_LOG_LEVEL=INFO
```

> **If using WSS:**  
> Set `STREAMER_WSS_AUTH_KEY` and `STREAMER_WSS_AUTH_USER` to protect your stream.

#### Example WSS Auth block:

```
STREAMER_WSS_AUTH_KEY=your_secret_key
STREAMER_WSS_AUTH_USER=your_username
```

The service checks these credentials for every incoming WSS connection.

---

### Running

Start the app (after editing `.env`):

```bash
poetry run python -m streamer
```

See the **Development** section below for Makefile commands.

---

## How it Works

1. **Bybit Connection:** Connect to Bybit WebSocket API for trades
2. **Kline Aggregation:** Bucket trades into candlesticks for all intervals
    > **Note:** The first candle of each interval is always skipped to ensure data consistency and avoid incomplete candles at startup.
3. **Consumer Dispatch:** Each enabled consumer receives finalized kline data
4. **Lifecycle:** Consumers are set up, started, receive data, and stopped in a managed order
5. **Any Combination:** Use any set of built-in or custom output consumers

---

## Trade Latency Considerations

Bybit may deliver trades with a delay, resulting in candlestick mismatches:

```
2025-12-30 19:03:05,000 - streamer.aggregator - DEBUG - Closing 1 candles at boundary 1767110585000
2025-12-30 19:03:05,001 - streamer.consumers.console - INFO - [XRPUSDT] [1000ms] timestamp=1767110584000 (2025-12-30 19:03:04) open=1.8755 high=1.8755 low=1.8755 close=1.8755 volume=0.0 trade_count=0
2025-12-30 19:03:05,865 - streamer.aggregator - DEBUG - Handle trade: {'topic': 'publicTrade.XRPUSDT', 'type': 'snapshot', 'ts': 1767110585795, 'data': [{'T': 1767110585794, 's': 'XRPUSDT', 'S': 'Buy', 'v': '363.1', 'p': '1.8755', 'L': 'ZeroMinusTick', 'i': '966f08b3-2f4a-56f1-af3e-5d60b701d12d', 'BT': False, 'RPI': False, 'seq': 225913470494}]}
2025-12-30 19:03:06,001 - streamer.aggregator - DEBUG - Closing 1 candles at boundary 1767110586000
2025-12-30 19:03:06,001 - streamer.consumers.console - INFO - [XRPUSDT] [1000ms] timestamp=1767110585000 (2025-12-30 19:03:05) open=1.8755 high=1.8755 low=1.8755 close=1.8755 volume=363.1 trade_count=1
2025-12-30 19:03:06,060 - streamer.aggregator - DEBUG - Handle trade: {'topic': 'publicTrade.XRPUSDT', 'type': 'snapshot', 'ts': 1767110585992, 'data': [{'T': 1767110585990, 's': 'XRPUSDT', 'S': 'Sell', 'v': '5.4', 'p': '1.8754', 'L': 'MinusTick', 'i': '0a1446cf-05eb-5c2e-a6d5-65d538f4d4c0', 'BT': False, 'RPI': False, 'seq': 225913470987}, {'T': 1767110585990, 's': 'XRPUSDT', 'S': 'Sell', 'v': '5.6', 'p': '1.8754', 'L': 'ZeroMinusTick', 'i': 'cf9d7317-89b2-5db9-8d00-dea3e8c4a670', 'BT': False, 'RPI': False, 'seq': 225913470987}]}
```

Trades (check timestamps above) may arrive after a candle boundary and thus get included in the _next_ candle.

---

### Aggregator Waiter Mode

To help with delivery delays, set `STREAMER_AGGREGATOR_WAITER_MODE_ENABLED=true`:

-   **Enabled:** Aggregator waits up to the configured latency (default 80ms) after candle close if any trades arrived for that candle.
-   **Disabled:** Candles close immediately; you may miss late trades.

Configure the wait latency with `STREAMER_AGGREGATOR_WAITER_LATENCY_MS` (in milliseconds).

**Recommended:** Enable waiter mode in production for data accuracy.

---

### Klines Mode

Klines mode provides more stable data but with higher latency compared to trade-based aggregation:

-   **Enabled (`STREAMER_KLINES_MODE=true`):** Uses Bybit's kline subscriptions for data. More stable candle data with ~1 second latency from Bybit.
-   **Disabled (`STREAMER_KLINES_MODE=false`):** Uses trade-based aggregation (default). Can be configured for low-latency data (80-150ms) using waiter mode.

When klines mode is enabled:
- Only specific intervals are supported (see `Interval.get_klines_mode_available_intervals()`)
- Waiter mode is automatically disabled (cannot be used together)
- Data is more reliable but has higher latency

#### Data Structures

**Klines Mode Response:**
```json
{
  "start": 1767621900000,
  "end": 1767621959999,
  "interval": "1",
  "open": "652.3",
  "close": "652.3",
  "high": "652.4",
  "low": "652.2",
  "volume": "0.23",
  "turnover": "150.029",
  "confirm": true,
  "timestamp": 1767621960179,
  "symbol": "BCHPERP"
}
```

**Trade-based Mode Response:**
```json
{
  "symbol": "PIPPINUSDT",
  "interval": 60000,
  "timestamp": 1767623520000,
  "open": 0.34193,
  "high": 0.34193,
  "low": 0.33916,
  "close": 0.33922,
  "volume": 220.0,
  "trade_count": 5
}
```

**Recommended:** Use klines mode for applications requiring data stability over low latency. Use trade-based mode with waiter for real-time applications.

---

## Extending

### Add Custom Consumers

Subclass the `BaseConsumer` interface:

```python
from streamer.consumers.base import BaseConsumer
from streamer.consumers import ConsumerManager

class MyCustomConsumer(BaseConsumer):
    def __init__(self, name: str = "custom") -> None:
        super().__init__(name)

    def validate(self) -> None:
        # Validation logic here
        pass

    async def setup(self) -> None:
        # Resource setup logic
        pass

    async def start(self) -> None:
        # Start logic
        self._is_running = True

    async def consume(self, data: List[Dict[str, Any]]) -> None:
        if not self._is_running:
            return
        # Your kline processing here

    async def stop(self) -> None:
        # Cleanup logic
        self._is_running = False
```

Register your consumer and enable it in `.env`:

```python
from streamer.consumers import ConsumerManager
ConsumerManager.register_consumer("mycustom", MyCustomConsumer)
# Add "mycustom" to STREAMER_ENABLED_CONSUMERS in your .env
```

#### Full Examples

See `examples/custom_consumer_example.py`:

-   File consumer (writes JSONL)
-   Console, Redis, and WebSocket consumers are built in and work out of the box

---

## Development

### Code Quality

-   **Ruff:** Fast linter/formatter
-   **MyPy:** Static type checker
-   **Black:** Code formatting (via Ruff)

### Commands

```bash
make install      # Install dependencies
make run          # Run the application
make check-all    # Run all checks (format, lint, mypy)
make lint         # Linter only
make format       # Formatter only
make mypy         # Type checker only
make clean        # Remove cache files
```

### Pre-commit Hooks

Before submitting changes:

```bash
pre-commit run --all-files
```

---

## License

MIT © Daniil Pavlovich
