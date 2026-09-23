"""Compression helper for the proxy (uses shared streamer codec logic)."""

from proxy.settings import settings
from streamer.compression import compress as _compress


def compress(data: bytes) -> bytes:
    """Compress bytes using the proxy's configured codec."""
    return _compress(data, settings.ws_compression)
