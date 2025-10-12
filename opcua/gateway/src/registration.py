import asyncio
import socket
import logging
import httpx
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
    Register this gateway with the coordinator and return the assigned gateway_id.

    The function retries registration until it successfully obtains a unique gateway_id.
    It checks that the assigned ID is not already used by another host and
    retries automatically if a conflict is detected, preventing race conditions when
    multiple gateways register at the same time.

    Args:
        logger (logging.Logger): Logger instance for messages.
        app (FastAPI): FastAPI app instance to store the gateway_id.

    Returns:
        str: The successfully assigned gateway ID.
    """
    g_host = await _get_gateway_url()

    while True:
        payload = {
            "gateway_id": app.state.gateway_id,
            "gateway_host": g_host,
            "devices": {uuid: {} for uuid in _active_device_defs.keys()},
        }

        try:
            # --- Try to contact the coordinator
            response = await app.state.http_client.post(f"{COORDINATOR_URL}/register_gateway", json=payload)
            response.raise_for_status()  # raises for 4xx/5xx

            data = response.json()
            gateway_id = data.get("gateway_id")

            # --- Verify ownership in KSQL
            try:
                rows = app.state.ksql.query(
                    f"SELECT GATEWAY_HOST FROM OPCUA_GATEWAYS WHERE GATEWAY_ID='{gateway_id}';"
                )
            except Exception as e:
                logger.error(f"Failed to query OPCUA_GATEWAYS table: {e}", exc_info=True)
                await asyncio.sleep(5)
                continue

            # --- Check for conflicts
            conflict = any(
                row.get("GATEWAY_HOST") and row["GATEWAY_HOST"] != g_host
                for row in rows
            )

            if conflict:
                logger.warning(
                    f"⚠️ Gateway ID conflict: {gateway_id} already registered with a different Gateway. Retrying..."
                )
                app.state.gateway_id = "UNAVAILABLE"
                await asyncio.sleep(2)
                continue

            # --- Success
            app.state.gateway_id = gateway_id
            logger.info(f"✅ Gateway registered successfully: {g_host} with ID {gateway_id}")
            return gateway_id

        except httpx.RequestError as e:
            logger.warning(f"Coordinator not reachable ({COORDINATOR_URL}): {e}")
            await asyncio.sleep(2)
        except httpx.HTTPStatusError as e:
            logger.warning(f"Coordinator returned HTTP error during registration: {e.response.status_code}")
        except Exception as e:
            logger.error(f"Unexpected error registering gateway {g_host}: {e}", exc_info=True)

        # Retry logic
        app.state.gateway_id = "UNAVAILABLE"
        await asyncio.sleep(5)


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
