"""Shared helpers for WebSocket message compression (gzip/zstd)."""

import gzip

import zstandard

GZIP_LEVEL = 5
ZSTD_LEVEL = 3


def compress(data: bytes, codec: str) -> bytes:
    """Compress bytes using the given codec ("gzip" or "zstd")."""
    if codec == "gzip":
        return gzip.compress(data, compresslevel=GZIP_LEVEL)
    if codec == "zstd":
        return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(data)
    raise ValueError(f"Unknown compression codec: {codec}")


def decompress(data: bytes) -> bytes:
    """Decompress bytes, auto-detecting the codec from magic bytes."""
    if data.startswith(b"\x1f\x8b"):
        return gzip.decompress(data)
    if data.startswith(b"\x28\xb5\x2f\xfd"):
        return zstandard.ZstdDecompressor().decompress(data)
    raise ValueError("Unknown compression format")
