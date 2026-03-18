"""Test websocket client."""

import asyncio
import gzip
import logging
import ssl

import orjson
import websockets

from streamer.settings import settings

# Set up logging
logger = logging.getLogger("websocket_test_client")
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def get_ws_uri() -> str:
    """
    Compose the WebSocket URI using settings or defaults.

    Returns:
        str: The WebSocket URI (ws:// or wss://).
    """
    host = settings.websocket_host or "localhost"
    port = settings.websocket_port or 8081
    path = settings.websocket_path or "/"
    scheme = "wss" if str(port).startswith("443") else "ws"
    return f"{scheme}://{host}:{port}{path}"


def get_ws_auth() -> dict[str, str]:
    """
    Get the WebSocket authentication payload from settings or defaults.

    Returns:
        dict[str, str]: Dictionary containing "user" and "key".
    """
    user = settings.wss_auth_user or "root"
    key = settings.wss_auth_key or "development"
    return {"user": user, "key": key}


async def main() -> None:
    """
    Test client for the websocket consumer running on localhost.

    Connects to the websocket server, sends authentication message using settings,
    and prints all incoming messages. If a binary message is received, attempt gzip decompression,
    then decode and parse as JSON, and print.
    """
    uri = get_ws_uri()
    auth_payload = get_ws_auth()
    logger.info(f"Connecting to websocket at {uri}")

    ssl_context = None
    if uri.startswith("wss://"):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    try:
        async with websockets.connect(uri, ssl=ssl_context) as ws:
            logger.info("Connected!")
            # Send authentication payload
            try:
                await ws.send(orjson.dumps(auth_payload).decode())
                logger.info(f"Sent authentication payload: {auth_payload}")
            except Exception as e:
                logger.error(f"Could not send auth payload: {e}")

            # Listen for messages
            while True:
                try:
                    msg = await ws.recv()
                    # If msg is bytes, try to decompress and decode as JSON
                    if isinstance(msg, bytes):
                        try:
                            decompressed = gzip.decompress(msg)
                            data = orjson.loads(decompressed)
                            logger.info(f"< [gzip-binary] Received:\n{data}")
                        except Exception as e:
                            logger.error(
                                f"< [binary payload] Failed to gunzip/decode: {e}"
                            )
                            # Print a summary only
                            preview = msg[:100]
                            logger.info(f"< [binary preview]: {preview}...")
                    else:
                        # msg is str (text)
                        try:
                            data = orjson.loads(msg)
                            logger.info(f"< Received:\n{data}")
                        except Exception:
                            # Not JSON - print raw
                            logger.info(
                                f"< [non-JSON (text) payload]: {msg[:100]}..."
                            )  # truncate if big
                except websockets.ConnectionClosed:
                    logger.info("Connection closed")
                    break
                except Exception as e:
                    logger.error(f"Error while receiving: {e}")
                    break
    except Exception as e:
        logger.error(f"Could not connect: {e}")


if __name__ == "__main__":
    asyncio.run(main())
