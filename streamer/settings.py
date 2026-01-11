"""Application settings using Pydantic."""

from typing import Annotated, Any, Set

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from streamer.types import Interval


class Settings(BaseSettings):
    """Application configuration settings."""

    # Bybit configuration
    bybit_symbols: Annotated[set[str], NoDecode] = {"BTCUSDT", "ETHUSDT"}
    bybit_load_all_symbols: bool = False
    bybit_symbols_limit: int | None = None
    bybit_socket_pool_size: int = 50
    kline_intervals: Annotated[Set[Interval], NoDecode] = set()

    # Streaming configuration
    enable_klines_stream: bool = True
    enable_price_stream: bool = False
    enable_ticker_stream: bool = False
    enable_tickers_kline_stream: bool = False
    enable_trades_stream: bool = False
    enable_spot_stream: bool = False

    # Redis configuration
    # (optional - only required when Redis consumer is enabled)
    redis_url: str | None = None
    redis_main_key: str | None = None

    # WebSocket server configuration
    # (optional - only required when WebSocket consumer is enabled)
    websocket_host: str | None = None
    websocket_port: int | None = None
    websocket_path: str = "/"

    # WebSocket authentication
    # (optional - only required when WebSocket consumer is enabled)
    wss_auth_key: str | None = None
    wss_auth_user: str | None = None

    # Consumer configuration
    enabled_consumers: Annotated[Set[str], NoDecode] = {"console", "redis", "websocket"}

    # Aggregator configuration
    aggregator_waiter_mode_enabled: bool = True
    aggregator_waiter_latency_ms: int = 80
    klines_mode: bool = False

    # Storage configuration
    storage_enabled: bool = False

    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: str = ""

    @field_validator("bybit_symbols", mode="before")
    @classmethod
    def validate_bybit_symbols(cls, value: Any) -> set[str]:
        """Parse string or list to set of symbols."""
        items: list[str] = []
        if isinstance(value, str):
            items.extend([v.strip() for v in value.split(",") if v.strip()])
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    items.extend([s.strip() for s in v.split(",") if s.strip()])
        return {x for x in items if x}

    @field_validator("kline_intervals", mode="before")
    @classmethod
    def validate_kline_intervals(cls, value: Any) -> set[Interval]:
        """Parse string or list to Interval instances with simple validation."""
        items: list[str] = []
        if isinstance(value, str):
            items.extend([v.strip() for v in value.split(",") if v.strip()])
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    items.extend([s.strip() for s in v.split(",") if s.strip()])
        return {Interval(x) for x in items if x}

    @field_validator("enabled_consumers", mode="before")
    @classmethod
    def validate_enabled_consumers(cls, value: Any) -> set[str]:
        """Parse string or list to a normalized set of enabled consumers."""
        items: list[str] = []
        if isinstance(value, str):
            items.extend([v.strip() for v in value.split(",") if v.strip()])
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    items.extend([s.strip() for s in v.split(",") if s.strip()])
        return {x for x in items if x}

    @model_validator(mode="after")
    def validate_klines_mode_intervals(self) -> "Settings":
        """Validate that all intervals are available in klines mode when enabled."""
        if self.klines_mode:
            available_intervals = Interval.get_klines_mode_available_intervals()
            invalid_intervals = {
                interval
                for interval in self.kline_intervals
                if interval not in available_intervals
            }
            if invalid_intervals:
                raise ValueError(
                    f"Invalid intervals for klines mode: {invalid_intervals}. "
                    f"Available intervals: {available_intervals}"
                )
            if self.aggregator_waiter_mode_enabled:
                raise ValueError(
                    "aggregator_waiter_mode_enabled cannot be True in klines mode; "
                    "set to False."
                )

        # Ensure at least one of the streaming options is enabled
        enabled_flags = [
            self.enable_klines_stream,
            self.enable_price_stream,
            self.enable_ticker_stream,
            self.enable_tickers_kline_stream,
            self.enable_trades_stream,
        ]
        if not any(enabled_flags):
            raise ValueError(
                "At least one of 'enable_klines_stream', "
                "'enable_price_stream', 'enable_ticker_stream', "
                "'enable_tickers_kline_stream', or 'enable_trades_stream' "
                "must be enabled."
            )

        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="STREAMER_",
        extra="forbid",
    )


settings = Settings()
