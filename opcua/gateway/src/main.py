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

from fastapi import FastAPI
import uvicorn
from .api import router as api_router
from .utils import setup_logging
from .config import OPCUA_GATEWAY_PORT, LOG_LEVEL

app = FastAPI(title="OPCUA Gateway", version="0.1")
app.logger = setup_logging(level=LOG_LEVEL)
app.include_router(api_router)


# ----------------------------
# Entry point for local dev
# ----------------------------
if __name__ == "__main__":
    uvicorn.run("opcua.gateway.src.main:app", host="0.0.0.0", port=OPCUA_GATEWAY_PORT, reload=True)
