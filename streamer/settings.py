"""Application settings using Pydantic."""

from typing import Annotated, Any, Literal, Set

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from yarl import URL

from streamer.types import Interval


class Settings(BaseSettings):
    """Application configuration settings."""

    exchange: Literal["bingx", "bybit"] = Field(default=...)
    # Exchange configuration
    exchange_symbols: Annotated[set[str], NoDecode] = {"BTCUSDT", "ETHUSDT"}
    exchange_load_all_symbols: bool = False
    exchange_symbols_limit: int | None = None
    exchange_socket_pool_size: int = 50
    kline_intervals: Annotated[Set[Interval], NoDecode] = set()

    # Streaming configuration
    enable_klines_stream: bool = True
    enable_price_stream: bool = False
    enable_ticker_stream: bool = False
    enable_tickers_kline_stream: bool = False
    enable_spot_stream: bool = False

    # Redis configuration
    # (optional - only required when Redis consumer is enabled)
    redis_host: str | None = None
    redis_port: int | None = None
    redis_user: str | None = None
    redis_password: str | None = None
    redis_base: int | None = None

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

    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: str = ""

    @field_validator("exchange_symbols", mode="before")
    @classmethod
    def validate_exchange_symbols(cls, value: Any) -> set[str]:
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
    def validate_settings(self) -> "Settings":
        """Validate settings configuration."""
        # Ensure at least one of the streaming options is enabled
        enabled_flags = [
            self.enable_klines_stream,
            self.enable_price_stream,
            self.enable_ticker_stream,
            self.enable_tickers_kline_stream,
        ]
        if not any(enabled_flags):
            raise ValueError(
                "At least one of 'enable_klines_stream', "
                "'enable_price_stream', 'enable_ticker_stream', "
                "'enable_tickers_kline_stream' "
                "must be enabled."
            )

        return self

    @property
    def redis_url(self) -> str:
        """
        Assemble REDIS URL from settings.

        :return: redis URL as string.
        """
        path = ""
        if self.redis_base is not None:
            path = f"/{self.redis_base}"
        if not self.redis_host:
            raise RuntimeError("redis_host is required to assemble the redis_url")
        url = URL.build(
            scheme="redis",
            host=self.redis_host,
            port=self.redis_port,
            user=self.redis_user,
            password=self.redis_password,
            path=path,
        )
        return str(url)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="STREAMER_",
        extra="ignore",
    )


settings = Settings()
