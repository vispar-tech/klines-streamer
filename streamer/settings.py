"""Application settings using Pydantic."""

from typing import Annotated, Any, List, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from streamer.symbols import load_all_symbols, validate_symbols
from streamer.types import Interval


class Settings(BaseSettings):
    """Application configuration settings."""

    # Bybit configuration
    bybit_symbols: Annotated[List[str], NoDecode] = ["BTCUSDT", "ETHUSDT"]
    bybit_load_all_symbols: bool = False
    bybit_symbols_limit: int | None = None
    kline_intervals: Annotated[List[Interval], NoDecode] = []

    # Redis configuration
    # (optional - only required when Redis consumer is enabled)
    redis_url: Optional[str] = None
    redis_channel: Optional[str] = None

    # WebSocket server configuration
    # (optional - only required when WebSocket consumer is enabled)
    websocket_host: Optional[str] = None
    websocket_port: Optional[int] = None
    websocket_url: Optional[str] = None

    # WebSocket authentication
    # (optional - only required when WebSocket consumer is enabled)
    wss_auth_key: Optional[str] = None
    wss_auth_user: Optional[str] = None

    # Consumer configuration
    enabled_consumers: List[str] = ["console"]

    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: str = ""

    @field_validator("kline_intervals", mode="before")
    @classmethod
    def validate_kline_intervals(cls, value: Any) -> list[Interval]:
        """Parse string or list to Interval instances with simple validation."""
        items: list[str] = []
        if isinstance(value, str):
            items.extend([v.strip() for v in value.split(",") if v.strip()])
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    items.extend([s.strip() for s in v.split(",") if s.strip()])
        return [Interval(x) for x in items if x]

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

    async def process_symbols(self) -> None:
        """
        Process and validate symbols based on configuration.

        This should be called after settings initialization to handle async operations.
        """
        if self.bybit_load_all_symbols:
            # Load all symbols from Bybit API
            all_symbols = await load_all_symbols(self.bybit_symbols_limit)
            self.bybit_symbols = list(all_symbols)
        else:
            # Validate provided symbols
            validated_symbols = await validate_symbols(self.bybit_symbols)
            self.bybit_symbols = validated_symbols

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="STREAMER_",
    )


settings = Settings()
