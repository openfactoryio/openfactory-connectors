import asyncio
import httpx
import socket
import logging
from fastapi import FastAPI
from .config import COORDINATOR_URL, OPCUA_GATEWAY_PORT
from .state import _active_device_defs

GATEWAY_RETRY_INTERVAL = 30  # seconds


async def _get_gateway_url():
    """ Return this gateway's host URL (used when registering with coordinator). """
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return f"http://{ip}:{OPCUA_GATEWAY_PORT}"


async def register_gateway_periodically(logger: logging.Logger, app: FastAPI) -> None:
    """
    Periodically register (or re-register) this gateway with the coordinator.
    This coroutine continuously sends the gateway's URL to the coordinator
    at fixed intervals defined by `GATEWAY_RETRY_INTERVAL`. It logs
    successes at the DEBUG level, warnings for non-200 responses, and errors
    for exceptions during the HTTP request.

    Args:
        logger (logging.Logger): Logger instance to record registration events.
        app (FastAPI): current application

    Raises:
        None: Exceptions are caught and logged internally.
    """
    g_host = await _get_gateway_url()

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                payload = {
                    "gateway_host": g_host,
                    "devices": {uuid: {} for uuid in _active_device_defs.keys()}
                }
                response = await client.post(f"{COORDINATOR_URL}/register_gateway", json=payload)
                if response.status_code == 200:
                    data = response.json()
                    app.gateway_id = data.get("gateway_id")
                    logger.debug(f"Re-registered gateway: {g_host}")
                else:
                    logger.warning(f"Failed to register gateway {g_host}, status={response.status_code}")
            except Exception as e:
                app.gateway_id = 'UNAVAILABLE'
                logger.error(f"Error registering gateway {g_host}: {e}")

            await asyncio.sleep(GATEWAY_RETRY_INTERVAL)
