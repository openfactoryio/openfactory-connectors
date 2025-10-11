import asyncio
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


async def register_gateway(logger: logging.Logger, app: FastAPI) -> str:
    """
    Register this gateway with the coordinator once and return the assigned gateway_id.

    Args:
        logger (logging.Logger): Logger instance for messages.
        app (FastAPI): FastAPI app instance to store the gateway_id.

    Returns:
        str: The assigned gateway ID.
    """
    g_host = await _get_gateway_url()

    try:
        payload = {
            "gateway_host": g_host,
            "devices": {uuid: {} for uuid in _active_device_defs.keys()}
        }
        response = await app.http_client.post(f"{COORDINATOR_URL}/register_gateway", json=payload)
        if response.status_code == 200:
            data = response.json()
            gateway_id = data.get("gateway_id")
            app.gateway_id = gateway_id
            logger.info(f"✅ Gateway registered: {g_host} with ID {gateway_id}")
            return gateway_id
        else:
            logger.warning(f"Failed to register gateway {g_host}, status={response.status_code}")
    except Exception as e:
        app.gateway_id = 'UNAVAILABLE'
        logger.error(f"Error registering gateway {g_host}: {e}")

    return 'UNAVAILABLE'


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
    while True:
        try:
            await register_gateway(logger, app)
        except Exception as e:
            logger.error(f"Error during periodic gateway registration: {e}")
        finally:
            await asyncio.sleep(GATEWAY_RETRY_INTERVAL)
