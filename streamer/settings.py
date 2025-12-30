"""Application settings using Pydantic."""

from typing import Annotated, Any, List, Set

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from streamer.types import Interval


class Settings(BaseSettings):
    """Application configuration settings."""

    # Bybit configuration
    bybit_symbols: Annotated[List[str], NoDecode] = ["BTCUSDT", "ETHUSDT"]
    bybit_load_all_symbols: bool = False
    bybit_symbols_limit: int | None = None
    bybit_socket_pool_size: int = 5
    kline_intervals: Annotated[Set[Interval], NoDecode] = set()

    # Redis configuration
    # (optional - only required when Redis consumer is enabled)
    redis_url: str | None = None
    redis_channel: str | None = None

    # WebSocket server configuration
    # (optional - only required when WebSocket consumer is enabled)
    websocket_host: str | None = None
    websocket_port: int | None = None
    websocket_url: str | None = None

    # WebSocket authentication
    # (optional - only required when WebSocket consumer is enabled)
    wss_auth_key: str | None = None
    wss_auth_user: str | None = None

    # Consumer configuration
    enabled_consumers: Annotated[List[str], NoDecode] = ["console"]

    # Aggregator configuration
    aggregator_waiter_mode_enabled: bool = True

    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: str = ""

    @field_validator("bybit_symbols", mode="before")
    @classmethod
    def validate_bybit_symbols(cls, value: Any) -> list[str]:
        """Parse string or list to list of symbols."""
        items: list[str] = []
        if isinstance(value, str):
            items.extend([v.strip() for v in value.split(",") if v.strip()])
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    items.extend([s.strip() for s in v.split(",") if s.strip()])
        return [x for x in items if x]

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
    def validate_enabled_consumers(cls, value: Any) -> list[str]:
        """Parse string or list to a normalized list of enabled consumers."""
        items: list[str] = []
        if isinstance(value, str):
            items.extend([v.strip() for v in value.split(",") if v.strip()])
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    items.extend([s.strip() for s in v.split(",") if s.strip()])
        return [x for x in items if x]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="STREAMER_",
    )


settings = Settings()
