# Bybit Klines Streamer

A Python service that **aggregates trades from Bybit’s WebSocket API into candlestick (kline) data** at multiple configurable intervals. The exchange-layer logic is hardcoded for Bybit and this repository is **not designed for implementing other exchanges**. However, the system does support **pluggable consumers**—you can extend outputs, but not sources.

> **Note:** If you use WSS output, you must configure **authorization and an access key** for clients connecting to the WebSocket server (see below).

---

## Features

-   **Aggregates trades to klines**: Converts raw trades into closed candle/bucket data.
-   **Multi-interval support**: Streams multiple candle/interval resolutions at once (as configured).
-   **Extendable, pluggable consumers**: Output klines to **Redis** or **WebSocket (WSS)**, or add your own consumer.
-   **Asyncio based**: High-performance, event-driven, and scalable.
-   **Easy configuration**: All core options are settable via `.env` or environment variables.

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

    Set your desired [Bybit symbols](https://bybit-exchange.github.io/docs/v5/intro), kline intervals (e.g. `1m,5m,1h`), and output targets.

    Example `.env`:

    ```
    BYBIT_SYMBOLS=BTCUSDT,ETHUSDT
    KLINE_INTERVALS=1m,5m,1h,4h,1d
    REDIS_URL=redis://localhost:6379/0
    WEBSOCKET_URL=wss://0.0.0.0:9500
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

4. **Run the streamer:**

    ```bash
    poetry run python -m streamer
    ```

---

## How it Works

-   Connects to Bybit’s WebSocket trade stream (hardcoded, not pluggable).
-   Buckets/aggregates all trades into klines (open, high, low, close, volume) for each configured interval.
-   Publishes closed klines to all enabled output channels (**Redis** and/or **WebSocket**) as soon as a candle interval ends.

---

## Extending

-   You can write new consumers by creating a class that implements the consumer protocol and registering it in the config.
-   All output, timing, and source parameters are adjustable from `.env`.
-   **Note:** Only Redis and WebSocket output backends are provided by default; input (exchange) code is Bybit-only and not abstracted.

---

## Development

-   Code style: [Ruff](https://github.com/charliermarsh/ruff) and [mypy](http://mypy-lang.org/) enforced.
-   Run `pre-commit run --all-files` before submitting changes.

---

## License

MIT © Daniil Pavlovich

---
