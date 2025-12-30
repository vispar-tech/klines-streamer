# Bybit Klines Streamer

A Python service that **aggregates trades from Bybit’s WebSocket API into candlestick (kline) data** at multiple configurable intervals. The exchange-layer logic is hardcoded for Bybit and this repository is **not designed for implementing other exchanges**. However, the system does support **pluggable consumers**—you can extend outputs, but not sources.

> **Note:** If you use WSS output, you must configure **authorization and an access key** for clients connecting to the WebSocket server (see below).

---

## Features

-   **Kline Aggregation**: Converts raw trades into candlestick (kline) data with open/high/low/close/volume
-   **Multi-Interval Support**: Streams multiple candle/interval resolutions simultaneously
-   **Pluggable Consumer System**: Extensible output architecture with Redis, WebSocket, and console consumers
-   **Automatic Configuration Validation**: Consumer-specific settings validation with helpful error messages
-   **Dynamic Symbol Management**: Load Bybit symbols or use custom lists with validation
-   **Type-Safe Settings**: Pydantic-based configuration with environment variable support
-   **Async Architecture**: Built with asyncio for high-performance concurrent operations
-   **Comprehensive Logging**: Structured logging throughout the application lifecycle
-   **Trade Latency Compensation**: Optional waiter mode to handle Bybit's trade delivery delays

---

## Usage

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

    Configure your consumer settings. The service supports multiple output destinations:

    Example `.env`:

    ```bash
    # Consumer selection (comma-separated list)
    ENABLED_CONSUMERS=console,redis,websocket

    # Redis settings (required only if Redis consumer is enabled)
    REDIS_URL=redis://localhost:6379/0
    REDIS_CHANNEL=klines

    # WebSocket settings (required only if WebSocket consumer is enabled)
    WEBSOCKET_HOST=localhost
    WEBSOCKET_PORT=9500
    WEBSOCKET_URL=wss://localhost:9500
    WSS_AUTH_KEY=your_secret_key
    WSS_AUTH_USER=your_username

    # Symbol configuration
    BYBIT_LOAD_ALL_SYMBOLS=false
    BYBIT_SYMBOLS=BTCUSDT,ETHUSDT
    BYBIT_SYMBOLS_LIMIT=
    BYBIT_SOCKET_POOL_SIZE=5

    # Kline intervals
    KLINE_INTERVALS=1m,5m,1h

    # Aggregator settings
    AGGREGATOR_WAITER_MODE_ENABLED=true

    # Logging
    LOG_LEVEL=INFO
    ```

    > **If using WSS (WebSocket) output:**  
    > Set environment variables for **WebSocket authorization and key** to protect your stream.  
    > Example:
    >
    > ```
    > WSS_AUTH_KEY=your_secret_key
    > WSS_AUTH_USER=your_username
    > ```
    >
    > The service checks these credentials for incoming WSS connections.

4. **Run the application:**

    ```bash
    # Run the application
    poetry run python -m streamer
    ```

    See the **Development** section below for additional Makefile commands.

---

## How it Works

The system aggregates Bybit trades into kline data and distributes through a consumer-based architecture:

1. **Bybit Connection**: Connects to Bybit WebSocket API for real-time trade data
2. **Kline Aggregation**: Buckets trades into candlesticks (open/high/low/close/volume) for configured intervals
3. **Consumer Processing**: Selected consumers process and distribute kline data
4. **Lifecycle Management**: Consumers go through setup → start → consume → stop phases
5. **Extensible Outputs**: Multiple consumer types can be combined for different output destinations

---

## Trade Latency Considerations

Bybit WebSocket API has inherent trade delivery delays that can cause OHLC data discrepancies:

```
2025-12-30 19:03:05,000 - streamer.aggregator - DEBUG - Closing 1 candles at boundary 1767110585000
2025-12-30 19:03:05,001 - streamer.consumers.console - INFO - [XRPUSDT] [1000ms] timestamp=1767110584000 (2025-12-30 19:03:04) open=1.8755 high=1.8755 low=1.8755 close=1.8755 volume=0.0 trade_count=0
2025-12-30 19:03:05,865 - streamer.aggregator - DEBUG - Handle trade: {'topic': 'publicTrade.XRPUSDT', 'type': 'snapshot', 'ts': 1767110585795, 'data': [{'T': 1767110585794, 's': 'XRPUSDT', 'S': 'Buy', 'v': '363.1', 'p': '1.8755', 'L': 'ZeroMinusTick', 'i': '966f08b3-2f4a-56f1-af3e-5d60b701d12d', 'BT': False, 'RPI': False, 'seq': 225913470494}]}
2025-12-30 19:03:06,001 - streamer.aggregator - DEBUG - Closing 1 candles at boundary 1767110586000
2025-12-30 19:03:06,001 - streamer.consumers.console - INFO - [XRPUSDT] [1000ms] timestamp=1767110585000 (2025-12-30 19:03:05) open=1.8755 high=1.8755 low=1.8755 close=1.8755 volume=363.1 trade_count=1
2025-12-30 19:03:06,060 - streamer.aggregator - DEBUG - Handle trade: {'topic': 'publicTrade.XRPUSDT', 'type': 'snapshot', 'ts': 1767110585992, 'data': [{'T': 1767110585990, 's': 'XRPUSDT', 'S': 'Sell', 'v': '5.4', 'p': '1.8754', 'L': 'MinusTick', 'i': '0a1446cf-05eb-5c2e-a6d5-65d538f4d4c0', 'BT': False, 'RPI': False, 'seq': 225913470987}, {'T': 1767110585990, 's': 'XRPUSDT', 'S': 'Sell', 'v': '5.6', 'p': '1.8754', 'L': 'ZeroMinusTick', 'i': 'cf9d7317-89b2-5db9-8d00-dea3e8c4a670', 'BT': False, 'RPI': False, 'seq': 225913470987}]}
```

As shown above, trades for the 6-second kline arrived at 19:03:06,060 after the candle boundary, causing the data to be included in the next candle instead of the correct one.

### Aggregator Waiter Mode

To mitigate this, enable **Aggregator Waiter Mode** with `AGGREGATOR_WAITER_MODE_ENABLED=true`:

-   **With waiter mode enabled**: When closing a candle that contains trades, the aggregator waits up to 80ms for potentially delayed trades before finalizing the OHLC data
-   **With waiter mode disabled**: Candles close immediately at interval boundaries, which is faster but may miss late-arriving trades

**Recommendation**: Keep waiter mode enabled for production use to ensure data accuracy, despite the slight performance impact.

---

## Extending

### Adding Custom Consumers

You can create custom consumers by implementing the `BaseConsumer` interface:

```python
from streamer.consumers.base import BaseConsumer
from streamer.consumers import ConsumerManager

class MyCustomConsumer(BaseConsumer):
    def __init__(self, name: str = "custom") -> None:
        super().__init__(name)

    def validate(self) -> None:
        # Add your validation logic here
        pass

    async def setup(self) -> None:
        # Initialize your consumer resources
        pass

    async def start(self) -> None:
        # Start your consumer
        self._is_running = True

    async def consume(self, data: List[Dict[str, Any]]) -> None:
        # Process the kline data
        if not self._is_running:
            return
        # Your logic here

    async def stop(self) -> None:
        # Cleanup resources
        self._is_running = False
```

Register your consumer:

```python
from streamer.consumers import ConsumerManager

# Register your consumer
ConsumerManager.register_consumer("mycustom", MyCustomConsumer)

# Add to ENABLED_CONSUMERS in .env
ENABLED_CONSUMERS=console,mycustom
```

### Examples

See `examples/custom_consumer_example.py` for complete examples of custom consumers including:

-   File consumer (writes to JSONL files)

Built-in consumers (ready to use without registration):

-   `console` - Prints data to stdout
-   `redis` - Publishes to Redis channels
-   `websocket` - Serves WebSocket connections

---

## Development

### Code Quality

The project uses comprehensive linting and type checking:

-   **Ruff**: Fast Python linter and formatter
-   **MyPy**: Static type checker
-   **Black**: Code formatter (via Ruff)

### Available Commands

```bash
# Install dependencies
make install

# Run the application
make run

# Code quality checks
make check-all    # Run all checks (format + lint + mypy)
make lint         # Run only linter
make format       # Run only formatter
make mypy         # Run only type checker

# Clean cache files
make clean
```

### Pre-commit Hooks

Run pre-commit hooks before submitting changes:

```bash
pre-commit run --all-files
```

---

## License

MIT © Daniil Pavlovich

---
