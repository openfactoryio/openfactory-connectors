# COORDINATOR_LOCAL_DEV=1 python -m opcua.coordinator.src.coordinator

import logging
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
from openfactory.schemas.devices import Device


# ----------------------------
# Logger setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("opcua-coordinator")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # This runs at app startup
    import os
    if os.getenv("COORDINATOR_LOCAL_DEV") == "1":
        gateways.append("http://localhost:8001")
        print("Added localhost gateway:", gateways)
    yield
    # App shutdown logic here if needed

app = FastAPI(title="OPCUA Coordinator", version="0.1", lifespan=lifespan)

# In-memory store: device_uuid -> gateway_id
device_assignments: Dict[str, str] = {}

# Gateway pool
gateways = []


class RegisterGatewayRequest(BaseModel):
    gateway_host: str


class RegisterDeviceRequest(BaseModel):
    device: Device


@app.post("/register_gateway")
async def register_gateway(req: RegisterGatewayRequest):
    """
    Register a new OPC UA Gateway dynamically.

    Request Body:
        req (RegisterGatewayRequest): The gateway registration payload containing `gateway_host`.

    Returns:
        dict: A confirmation with the registered gateway host.
    """
    gateway_host = req.gateway_host
    if gateway_host not in gateways:
        gateways.append(gateway_host)
    logger.info(f"Registred new gateway {gateway_host}")
    return {"status": "registered", "gateway_host": gateway_host}


@app.post("/register_device")
async def register_device(req: RegisterDeviceRequest):
    if not gateways:
        raise HTTPException(status_code=500, detail="No gateways registered")

    device_uuid = req.device.uuid

    if device_uuid in device_assignments:
        raise HTTPException(status_code=400, detail=f"Device {device_uuid} already registered.")

    # Simple round-robin assignment
    assigned_gateway = gateways[len(device_assignments) % len(gateways)]
    device_assignments[device_uuid] = assigned_gateway
    logger.info(f"Device {device_uuid} assigned to {assigned_gateway}")

    # Notify gateway
    url = f"{assigned_gateway}/add_device"
    payload = {"device": req.device.model_dump()}
    try:
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        logger.info("✅ Success:", resp.json())
    except Exception as e:
        logger.error("❌ Failed:", e)

    return {"device_uuid": device_uuid, "assigned_gateway": assigned_gateway}


@app.get("/assignments")
async def get_assignments():
    """ Return all device -> gateway assignments. """
    return device_assignments


@app.get("/gateways")
async def get_gateways():
    """ Return all gateway assignments. """
    return gateways


# ----------------------------
# Entry point for local dev
# ----------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "opcua.coordinator.src.coordinator:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
