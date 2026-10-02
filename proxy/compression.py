"""Compression helper for the proxy (self-contained codec logic)."""

import gzip

import zstandard

from proxy.settings import settings

GZIP_LEVEL = 5
ZSTD_LEVEL = 3


def compress(data: bytes) -> bytes:
    """Compress bytes using the proxy's configured codec."""
    codec = settings.ws_compression
    if codec == "gzip":
        return gzip.compress(data, compresslevel=GZIP_LEVEL)
    if codec == "zstd":
        return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(data)
    raise ValueError(f"Unknown compression codec: {codec}")
