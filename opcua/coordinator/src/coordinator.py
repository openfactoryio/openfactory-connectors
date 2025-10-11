import os
import logging
import time
import asyncio
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict

from openfactory.kafka import KSQLDBClient
from openfactory.assets import Asset, AssetAttribute
from openfactory.schemas.devices import Device
from openfactory.utils import register_asset, deregister_asset


# ----------------------------
# Logger setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("opcua-coordinator")


# ----------------------------
# In-memory store
# ----------------------------
# device_uuid -> gateway_host
device_assignments: Dict[str, str] = {}

# gateway_host -> {"last_seen": float, "devices": {device_uuid: ...}}
gateways_info: Dict[str, Dict] = {}


# ----------------------------
# Async helper functions
# ----------------------------
async def create_asset_async(asset_uuid: str, asset_type="OpenFactoryApp") -> Asset:
    """
    Asynchronously register and create an OpenFactory Asset.

    This coroutine runs the synchronous `register_asset` function in a background thread,
    allowing non-blocking registration of the asset within the event loop. It then
    instantiates and returns an `Asset` object configured with the same UUID.

    Args:
        asset_uuid (str): The unique identifier (UUID) for the asset to register.
        asset_type (str, optional): The asset type to register, defaults to "OpenFactoryApp".

    Returns:
        Asset: The created `Asset` instance associated with the given `asset_uuid`.

    Raises:
        Exception: Propagates any exception raised by the underlying registration call.
    """
    await asyncio.to_thread(register_asset, asset_uuid, None, asset_type, ksql, os.getenv("KAFKA_BROKER"))
    asset = Asset(asset_uuid=asset_uuid, ksqlClient=ksql, bootstrap_servers=os.getenv("KAFKA_BROKER"))
    return asset


async def deregister_asset_async(asset_uuid: str):
    """
    Asynchronously deregister an existing OpenFactory Asset.

    This coroutine executes the synchronous `deregister_asset` function in a background
    thread to prevent blocking the event loop. It safely removes the asset definition
    from OpenFactory.

    Args:
        asset_uuid (str): The unique identifier (UUID) of the asset to deregister.

    Returns:
        None

    Raises:
        Exception: Propagates any exception raised during the deregistration process.
    """
    await asyncio.to_thread(deregister_asset, asset_uuid, ksqlClient=ksql)


async def _cleanup_expired_gateways():
    """
    Remove gateways that haven't re-registered within GATEWAY_TIMEOUT seconds.
    Also remove their device assignments.
    """
    GATEWAY_TIMEOUT = 60
    try:
        while True:
            now = time.time()
            for g_host, info in list(gateways_info.items()):
                if now - info["last_seen"] > GATEWAY_TIMEOUT:
                    logger.warning(f"Gateway {g_host} timed out, removing.")
                    for dev, gw in list(device_assignments.items()):
                        if gw == g_host:
                            del device_assignments[dev]
                            asyncio.create_task(deregister_asset_async(dev))
                            asyncio.create_task(deregister_asset_async(dev + '-PRODUCER'))
                    del gateways_info[g_host]
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.info("Cleanup task cancelled gracefully")
        # Optionally do final cleanup here

        raise  # Re-raise to properly signal cancellation


def _update_device_assignments_from_gateway(gateway_host: str, devices: Dict[str, dict]):
    """
    Rebuild device_assignments mapping from a gateway's reported devices.
    """
    for dev_uuid in devices.keys():
        device_assignments[dev_uuid] = gateway_host


# ----------------------------
# FastAPI lifespan
# ----------------------------
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
    if os.getenv("COORDINATOR_LOCAL_DEV") == "1":
        # register a dummy localhost gateway for dev
        gateways_info["http://localhost:8001"] = {"devices": {}, "last_seen": time.time()}
        print("Added localhost gateway:", list(gateways_info.keys()))

    # Fully async coordinator registration
    global coordinator
    coordinator = await create_asset_async("OPCUA-COORDINATOR")
    coordinator.add_attribute(
        AssetAttribute(id='avail', value="AVAILABLE", tag="Availability", type="Events")
    )
    coordinator.add_attribute(
        AssetAttribute(id='application_manufacturer', value='OpenFactoryIO', type='Events', tag='Application.Manufacturer')
    )
    coordinator.add_attribute(
        AssetAttribute(id='application_license', value='Polyform Noncommercial License 1.0.0', type='Events', tag='Application.License')
    )
    coordinator.add_attribute(
        AssetAttribute(id='application_version', value=os.environ.get('APPLICATION_VERSION'), type='Events', tag='Application.Version')
    )

    # Start cleanup coroutine
    cleanup_task = asyncio.create_task(_cleanup_expired_gateways())

    try:
        # Yield control to FastAPI
        yield
    except SystemExit:
        logger.info("SystemExit caught during shutdown")
    finally:
        # Update coordinator availability before shutting down
        try:
            coordinator.add_attribute(AssetAttribute(id='avail', value="UNAVAILABLE", tag="Availability", type="Events"))
            logger.info("Coordinator marked UNAVAILABLE")
        except Exception as e:
            logger.error(f"Failed to mark coordinator UNAVAILABLE: {e}")

        # Cancel and await cleanup task
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            logger.info("Cleanup task cancelled on shutdown")


# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="OPCUA Coordinator", version=os.environ.get('APPLICATION_VERSION'), lifespan=lifespan)
ksql = KSQLDBClient(os.getenv("KSQLDB_URL"))


# ----------------------------
# Pydantic models
# ----------------------------
class RegisterGatewayRequest(BaseModel):
    gateway_host: str
    devices: Dict[str, dict] | None = None


class RegisterDeviceRequest(BaseModel):
    device: Device


# ----------------------------
# Endpoints
# ----------------------------
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

    is_new = gateway_host not in gateways_info

    # Update gateway info
    gateways_info[gateway_host] = {
        "last_seen": time.time(),
        "devices": req.devices or gateways_info.get(gateway_host, {}).get("devices", {}),
    }

    if is_new:
        index = len(gateways_info)
        gateway_uuid = f"OPCUA-GATEWAY-{index}"
        coordinator.add_reference_below(gateway_uuid)

        # Async registration
        gateway = await create_asset_async(gateway_uuid)
        gateway.add_attribute(AssetAttribute(id='avail', value="AVAILABLE", tag="Availability", type="Events"))
        gateway.add_attribute(AssetAttribute(id='uri', value=gateway_host, tag="GatewayURI", type="Events"))
        gateway.add_attribute(AssetAttribute(id='application_manufacturer', value='OpenFactoryIO', type='Events', tag='Application.Manufacturer'))
        gateway.add_attribute(AssetAttribute(id='application_license', value='Polyform Noncommercial License 1.0.0', type='Events', tag='Application.License'))
        gateway.add_attribute(AssetAttribute(id='application_version', value=os.environ.get('APPLICATION_VERSION'), type='Events', tag='Application.Version'))

        logger.info(f"Registered new gateway {gateway_host}")
    else:
        logger.info(f"Refreshed gateway {gateway_host} (periodic re-registration)")

    # Update device assignments from this gateway's reported devices
    if req.devices:
        _update_device_assignments_from_gateway(gateway_host, req.devices)

    return {
        "status": "registered",
        "gateway_host": gateway_host,
        "device_count": len(req.devices or {}),
        "known_gateways": len(gateways_info),
    }


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
    if not gateways_info:
        raise HTTPException(status_code=500, detail="No gateways registered")

    device_uuid = req.device.uuid

    if device_uuid in device_assignments:
        raise HTTPException(status_code=400, detail=f"Device {device_uuid} already registered.")

    # Round-robin assignment
    gateway_hosts = list(gateways_info.keys())
    assigned_gateway = gateway_hosts[len(device_assignments) % len(gateway_hosts)]
    device_assignments[device_uuid] = assigned_gateway
    logger.info(f"Device {device_uuid} assigned to {assigned_gateway}")

    async def notify_gateway():
        url = f"{assigned_gateway}/add_device"
        payload = {"device": req.device.model_dump()}
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=payload, timeout=5)
                resp.raise_for_status()
                logger.info(f"✅ Gateway notified: {url} response={resp.status_code}")
        except Exception as e:
            logger.error(f"❌ Failed to notify gateway {url}: {e}")

    async def register_producer():
        try:
            producer = await create_asset_async(device_uuid.upper() + '-PRODUCER')
            producer.add_attribute(AssetAttribute(
                id='opcua-gateway',
                value=assigned_gateway,
                type='Events',
                tag='ProducerURI'
            ))
            logger.info(f"✅ Producer registered for {device_uuid}")
        except Exception as e:
            logger.error(f"❌ Failed to register producer for {device_uuid}: {e}")

    # Run both tasks concurrently and wait for them to finish
    async with asyncio.TaskGroup() as tg:
        tg.create_task(notify_gateway())
        tg.create_task(register_producer())

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
    del device_assignments[device_uuid]

    async def notify_remove():
        url = f"{assigned_gateway}/remove_device/{device_uuid}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.delete(url, timeout=5)
                resp.raise_for_status()
                logger.info(f"✅ Gateway notified to remove {device_uuid}: {url} response={resp.status_code}")
        except Exception as e:
            logger.error(f"❌ Failed to notify {assigned_gateway} for {device_uuid}: {e}")

    async def deregister_producer():
        try:
            await deregister_asset_async(device_uuid.upper() + '-PRODUCER')
            producer = await create_asset_async(device_uuid.upper() + '-PRODUCER')
            producer.add_attribute(AssetAttribute(
                id='opcua-gateway',
                value='UNAVAILABLE',
                type='Events',
                tag='ProducerURI'
            ))
            logger.info(f"✅ Producer deregistered for {device_uuid}")
        except Exception as e:
            logger.error(f"❌ Failed to deregister producer for {device_uuid}: {e}")

    # Run both async tasks concurrently and wait for them to finish
    async with asyncio.TaskGroup() as tg:
        tg.create_task(notify_remove())
        tg.create_task(deregister_producer())

    logger.info(f"Device {device_uuid} unregistered locally from {assigned_gateway}")

    return {
        "status": "unregistered",
        "device_uuid": device_uuid,
        "gateway": assigned_gateway
    }


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
    return {
        gateway: {"device_count": len(info["devices"]), "last_seen": info["last_seen"]}
        for gateway, info in gateways_info.items()
    }


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
