"""Proxy application settings."""

from typing import Annotated, Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class ProxySettings(BaseSettings):
    """Proxy configuration settings."""

    # WebSocket server configuration
    websocket_host: str = "localhost"
    websocket_port: int = 8080
    websocket_path: str = "/"

    # Upstream WebSocket connections
    upstream_connections: Annotated[list[str], NoDecode] = [
        "ws://streamer-bybit:9090/",
        "ws://streamer-bingx:9090/",
        "ws://streamer-bitget:9090/",
    ]

    # Authentication (same as streamer)
    wss_auth_user: str = "root"
    wss_auth_key: str = "development"

    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: str = ""

    @field_validator("upstream_connections", mode="before")
    @classmethod
    def validate_upstream_connections(cls, value: Any) -> list[str]:
        """Parse string or list to list of upstream URLs."""
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
        env_prefix="PROXY_",
        extra="ignore",
    )


settings = ProxySettings()
