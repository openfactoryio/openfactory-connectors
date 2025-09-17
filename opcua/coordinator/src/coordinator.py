# python -m opcua.coordinator.src.coordinator


from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
from openfactory.schemas.devices import Device

app = FastAPI(title="OPCUA Coordinator", version="0.1")

# In-memory store: device_uuid -> gateway_id
device_assignments: Dict[str, str] = {}

# Example Gateway pool (in a real system this would be dynamic)
gateways = ["opcua-gateway-1", "opcua-gateway-2"]


class RegisterDeviceRequest(BaseModel):
    device: Device


@app.post("/register_device")
async def register_device(req: RegisterDeviceRequest):
    device_uuid = req.device.uuid

    if device_uuid in device_assignments:
        raise HTTPException(status_code=400, detail=f"Device {device_uuid} already registered.")

    # Simple round-robin assignment
    assigned_gateway = gateways[len(device_assignments) % len(gateways)]
    device_assignments[device_uuid] = assigned_gateway

    # TODO: notify gateway (Kafka or API call)
    print(f"Device {device_uuid} assigned to {assigned_gateway}")

    return {"device_uuid": device_uuid, "assigned_gateway": assigned_gateway}


@app.get("/assignments")
async def get_assignments():
    """ Return all device -> gateway assignments. """
    return device_assignments


# ----------------------------
# ENTRY POINT
# ----------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "opcua.coordinator.src.coordinator:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # only for development
    )
