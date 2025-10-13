"""
Main entry point for the OPC UA Gateway FastAPI application.

This module initializes the FastAPI app, sets up logging, includes
API routes, and starts the Uvicorn server when run as a script.

Key components:
- `app`: FastAPI instance with attached logger (`app.state.logger`).
- API routes imported from the `api` module.
- Logging configured via `setup_logging()` with level from config.
- Runs Uvicorn server on `OPCUA_GATEWAY_PORT` when executed directly.

To run the gateway:

    python -m opcua.gateway.src.main
"""

import os
import asyncio
import uvicorn
import logging
import json
import httpx
from fastapi import FastAPI
from contextlib import asynccontextmanager
from openfactory.kafka import KSQLDBClient
from openfactory.schemas.devices import Device
from .registration import register_gateway
from .api import router as api_router
from .utils import setup_logging
from .config import OPCUA_GATEWAY_PORT, LOG_LEVEL
from .state import _active_device_defs, _active_tasks
from .monitor import monitor_device


async def rebuild_gateway_state(logger: logging.Logger, gateway_id: str) -> None:
    """
    Rebuild in-memory gateway state from ksqlDB based on OPC UA assignments.

    Only devices assigned to this gateway (from OPCUA_DEVICE_ASSIGNMENT) are loaded.

    Args:
        logger (logging.Logger): Logger instance.
        gateway_id (str): The identifier of the current gateway.
    """
    logger.info(f"Rebuilding gateway state for {gateway_id} from ksqlDB...")

    def _fetch_assigned_devices():
        """ Blocking I/O to fetch assigned device UUIDs. """
        query = f"""
        SELECT DEVICE_UUID
        FROM OPCUA_DEVICE_ASSIGNMENT
        WHERE GATEWAY_ID = '{gateway_id}';
        """
        return app.state.ksql.query(query)

    def _fetch_device_configs(device_uuids: list[str]):
        """ Blocking I/O to fetch connector configs for the given devices. """
        if not device_uuids:
            return []
        uuids_str = ",".join(f"'{u}'" for u in device_uuids)
        query = f"""
        SELECT ASSET_UUID, CONNECTOR_CONFIG
        FROM DEVICE_CONNECTOR
        WHERE ASSET_UUID IN ({uuids_str});
        """
        return app.state.ksql.query(query)

    try:
        # Get assigned device UUIDs
        assigned_rows = await asyncio.to_thread(_fetch_assigned_devices)
        assigned_uuids = [r["DEVICE_UUID"] for r in assigned_rows if r.get("DEVICE_UUID")]
        logger.info(f"Found {len(assigned_uuids)} devices assigned to this gateway.")

        if not assigned_uuids:
            logger.info("No devices assigned; nothing to rebuild.")
            return

        # Fetch their connector configs
        config_rows = await asyncio.to_thread(_fetch_device_configs, assigned_uuids)
        logger.debug(f"Fetched {len(config_rows)} connector configs from DEVICE_CONNECTOR.")

        # Populate _active_device_defs
        _active_device_defs.clear()
        for row in config_rows:
            dev_uuid = row.get("ASSET_UUID")
            raw_config = row.get("CONNECTOR_CONFIG")

            if not dev_uuid or not raw_config:
                logger.warning("Skipping malformed ksqlDB row: %s", row)
                continue

            try:
                cfg = json.loads(raw_config)
                device = Device(**cfg)
                _active_device_defs[dev_uuid] = device
                logger.debug(f"Loaded device {dev_uuid} into gateway state.")
                task = asyncio.create_task(monitor_device(device, logger))
                _active_tasks[device.uuid] = task
            except Exception as e:
                logger.warning(f"Failed to connect device {dev_uuid}: {e}", exc_info=True)

        logger.info(f"✅ Restored {_active_device_defs.__len__()} devices for gateway {gateway_id}.")

    except Exception as e:
        logger.error(f"❌ Error rebuilding gateway state for {gateway_id}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    Starts the periodic gateway registration task and cancels it cleanly at shutdown.

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None
    """
    # Register with coordinator
    app.state.logger.info('Waiting for OPC UA Coordinator registration ...')
    try:
        await register_gateway(app.state.logger, app)
        app.state.logger.info(f'Registered Gateway as {app.state.gateway_id}')
    except Exception as e:
        app.state.logger.error(f"Failed to register gateway at startup: {e}")

    # Rebuild local in-memory state before starting anything ---
    await rebuild_gateway_state(app.state.logger, app.state.gateway_id)

    yield  # <-- control returns to FastAPI while the app is running

    # App shutdown logic


app = FastAPI(title="OPCUA Gateway",
              version=os.environ.get('APPLICATION_VERSION'),
              lifespan=lifespan)
app.state.logger = setup_logging(level=LOG_LEVEL)
app.state.gateway_id = 'UNAVAILABLE'
app.state.ksql = KSQLDBClient(os.getenv("KSQLDB_URL"))
app.state.http_client = httpx.AsyncClient(timeout=10.0)
app.include_router(api_router)


# ----------------------------
# Entry point for local dev
# ----------------------------
if __name__ == "__main__":
    uvicorn.run("opcua.gateway.src.main:app", host="0.0.0.0", port=OPCUA_GATEWAY_PORT, reload=True)
