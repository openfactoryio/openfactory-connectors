"""
Main entry point for the OPC UA Gateway FastAPI application.

This module initializes the FastAPI app, sets up logging, includes
API routes, and starts the Uvicorn server when run as a script.

Key components:
- `app`: FastAPI instance with attached logger (`app.logger`).
- API routes imported from the `api` module.
- Logging configured via `setup_logging()` with level from config.
- Runs Uvicorn server on `OPCUA_GATEWAY_PORT` when executed directly.

To run the gateway:

    python -m opcua.gateway.src.main
"""

import os
import asyncio
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from .registration import register_gateway_periodically
from .api import router as api_router
from .utils import setup_logging
from .config import OPCUA_GATEWAY_PORT, LOG_LEVEL


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
    task = asyncio.create_task(register_gateway_periodically(app.logger))

    yield  # <-- control returns to FastAPI while the app is running

    # App shutdown logic
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        app.logger.info("Gateway registration task cancelled.")


app = FastAPI(title="OPCUA Gateway",
              version=os.environ.get('APPLICATION_VERSION'),
              lifespan=lifespan)
app.logger = setup_logging(level=LOG_LEVEL)
app.include_router(api_router)


# ----------------------------
# Entry point for local dev
# ----------------------------
if __name__ == "__main__":
    uvicorn.run("opcua.gateway.src.main:app", host="0.0.0.0", port=OPCUA_GATEWAY_PORT, reload=True)
