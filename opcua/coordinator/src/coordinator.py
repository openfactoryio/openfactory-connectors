# COORDINATOR_LOCAL_DEV=1 python -m opcua.coordinator.src.coordinator

import os
import logging
import requests
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
from confluent_kafka import Producer
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute
from openfactory.schemas.devices import Device


# ----------------------------
# Logger setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("opcua-coordinator")


class KafkaProducer(Producer):

    def __init__(self):
        super().__init__({'bootstrap.servers': os.getenv("KAFKA_BROKER")})
        self.ksql = KSQLDBClient(os.getenv("KSQLDB_URL"))
        self.topic = self.ksql.get_kafka_topic("ASSETS_STREAM")

    def send(self, asset_uuid: str, asset_attribute: AssetAttribute) -> None:
        """
        Send a Kafka message for the given asset and attribute.

        Args:
            asset_uuid (str): UUID of the asset.
            asset_attribute (AssetAttribute): Attribute data to send.

        Returns:
            None
        """
        msg = {
            "ID": asset_attribute.id,
            "VALUE": asset_attribute.value,
            "TAG": asset_attribute.tag,
            "TYPE": asset_attribute.type,
            "attributes": {"timestamp": asset_attribute.timestamp}
        }
        self.produce(topic=self.topic, key=asset_uuid, value=json.dumps(msg))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    This function runs at application startup and shutdown. In local development,
    it automatically registers a default gateway ("http://localhost:8001") if
    the `COORDINATOR_LOCAL_DEV` environment variable is set to "1".

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None
    """
    # This runs at app startup
    import os
    if os.getenv("COORDINATOR_LOCAL_DEV") == "1":
        gateways.append("http://localhost:8001")
        print("Added localhost gateway:", gateways)
    yield
    # App shutdown logic here if needed

app = FastAPI(title="OPCUA Coordinator", version="0.1", lifespan=lifespan)
app.kafka_producer = KafkaProducer()

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

    Args:
        req (RegisterGatewayRequest): The gateway registration payload containing the `gateway_host` URL.

    Returns:
        dict: A confirmation dictionary with keys:
            - "status": Registration status ("registered")
            - "gateway_host": The registered gateway host URL
    """
    gateway_host = req.gateway_host
    if gateway_host not in gateways:
        gateways.append(gateway_host)
    logger.info(f"Registred new gateway {gateway_host}")
    return {"status": "registered", "gateway_host": gateway_host}


@app.post("/register_device")
async def register_device(req: RegisterDeviceRequest):
    """
    Register a new OPC UA device and assign it to a gateway.

    Performs a round-robin assignment of devices to available gateways. Notifies
    the assigned gateway via an HTTP POST request to add the device.

    Args:
        req (RegisterDeviceRequest): Payload containing the device information.

    Raises:
        HTTPException: If no gateways are registered or the device is already registered.

    Returns:
        dict: Assignment details with keys:
            - "device_uuid": UUID of the registered device
            - "assigned_gateway": Host URL of the assigned gateway
    """
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

    # register gateway with Producer Asset
    app.kafka_producer.send(
        asset_uuid=f"{device_uuid}-PRODUCER",
        asset_attribute=AssetAttribute(
            id='opcua-gateway',
            value=assigned_gateway,
            type='Events',
            tag='ProducerURI'
        )
    )

    return {"device_uuid": device_uuid, "assigned_gateway": assigned_gateway}


@app.delete("/unregister_device/{device_uuid}")
async def unregister_device(device_uuid: str):
    """
    Unregister a device and notify its assigned gateway to remove it.

    Args:
        device_uuid (str): UUID of the device to unregister.

    Raises:
        HTTPException: If the device is not registered or gateway call fails.

    Returns:
        dict: Status of the unregistration with keys:
            - "status": Unregistration status ("unregistered")
            - "device_uuid": UUID of the unregistered device
            - "gateway": Host URL of the gateway that was notified
    """
    if device_uuid not in device_assignments:
        raise HTTPException(status_code=404, detail=f"Device {device_uuid} not found")

    assigned_gateway = device_assignments[device_uuid]

    # Notify gateway
    url = f"{assigned_gateway}/remove_device/{device_uuid}"
    try:
        resp = requests.delete(url)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"❌ Failed to remove {device_uuid} from {assigned_gateway}: {e}")
        raise HTTPException(status_code=500, detail="Failed to contact gateway")

    # Remove from registry
    del device_assignments[device_uuid]
    logger.info(f"Device {device_uuid} unregistered from {assigned_gateway}")

    return {"status": "unregistered", "device_uuid": device_uuid, "gateway": assigned_gateway}


@app.get("/assignments")
async def get_assignments():
    """
    Retrieve all device-to-gateway assignments.

    Returns:
        Dict[str, str]: A dictionary mapping device UUIDs to assigned gateway hosts.
    """
    return device_assignments


@app.get("/gateways")
async def get_gateways():
    """
    Retrieve the list of all registered gateways.

    Returns:
        List[str]: A list of gateway host URLs currently registered with the coordinator.
    """
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
