#!/usr/bin/env python3
"""Example WebSocket client for the proxy server."""

import gzip
import os
import ssl
from datetime import datetime

import orjson
from websockets.asyncio.client import connect


def log(msg: str) -> None:
    """Print msg with time."""
    now = datetime.now()
    print(f"{now:%Y-%m-%d %H:%M:%S}.{now.microsecond // 1000:03d} {msg}")


async def main() -> None:
    """Connect to proxy and consume aggregated data."""
    uri = os.getenv("PROXY_URI", "ws://localhost:8080/")

    log(f"Connecting to proxy at: {uri}")

    # Create an SSL context that disables SSL verification
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Decide SSL argument for connect based on URI scheme
    ssl_arg = ssl_context if uri.startswith("wss://") else None

    try:
        async with connect(uri, ssl=ssl_arg) as websocket:
            log("Connected to proxy")

            # Authenticate
            auth_message = orjson.dumps({"user": "root", "key": "development"}).decode()
            await websocket.send(auth_message)

            # Wait for authentication response
            response = await websocket.recv()
            auth_response = orjson.loads(response)
            log(f"Auth status: {auth_response.get('status')}")

            if auth_response.get("status") != "authenticated":
                log("Authentication failed")
                return

            # Listen for data
            log("Listening for aggregated kline data...")
            async for message in websocket:
                try:
                    if isinstance(message, str):
                        data = orjson.loads(message)
                        if data.get("type") == "welcome":
                            log("type=welcome msg=Welcome received")
                        else:
                            exchange = data.get("exchange", "unknown")
                            channel = data.get("channel", "unknown")
                            data_type = data.get("data_type", "unknown")
                            data_len = len(data.get("data", []))
                            log(
                                f"type=initial_data exchange={exchange} "
                                f"channel={channel} data_type={data_type} "
                                f"len={data_len}"
                            )
                        continue

                    decompressed = gzip.decompress(message)
                    data = orjson.loads(decompressed)

                    exchange = data.get("exchange", "unknown")
                    channel = data.get("channel", "unknown")
                    data_type = data.get("data_type", "unknown")
                    data_len = len(data.get("data", []))

                    log(
                        f"exchange={exchange} channel={channel} "
                        f"data_type={data_type} len={data_len}"
                    )

                except Exception as e:
                    log(f"msg=ErrorProcessingMessage error={e}")

    except Exception as e:
        log(f"msg=ConnectionError error={e}")
