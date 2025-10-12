import os
import logging
import time
import asyncio
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import TypedDict, Dict, Set
from confluent_kafka import Producer

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
class GatewayInfo(TypedDict):
    last_seen: float
    url: str
    devices: Set[str]


# gateway_info -> {"last_seen": float, "devices": set[str]}
# dict key is the OPC UA Gateway UUID, (OPCUA-GATEWAY-1, OPCUA-GATEWAY-2, ...)
gateways_info: Dict[str, GatewayInfo] = {}


# ----------------------------
# Dynamic semaphore limit based on CPU count
# ----------------------------
CPU_COUNT = os.cpu_count() or 1
THREADS_PER_CPU = int(os.getenv("THREADS_PER_CPU", "4"))
MAX_THREAD_TASK_LIMIT = int(os.getenv("MAX_THREAD_TASK_LIMIT", 32))
THREAD_TASK_LIMIT = min(CPU_COUNT * THREADS_PER_CPU, MAX_THREAD_TASK_LIMIT)
thread_semaphore = asyncio.Semaphore(THREAD_TASK_LIMIT)
logger.info(f"Dynamic thread limit: {THREAD_TASK_LIMIT} (CPUs: {CPU_COUNT}, per CPU: {THREADS_PER_CPU}, max: {MAX_THREAD_TASK_LIMIT})")


# ----------------------------
# Helper functions
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
    async with thread_semaphore:
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
    async with thread_semaphore:
        await asyncio.to_thread(deregister_asset, asset_uuid, ksqlClient=ksql)


async def _cleanup_expired_gateways():
    """
    Remove gateways that haven't re-registered within GATEWAY_TIMEOUT seconds.
    Also deregister all devices that belonged to that gateway, in batches for safety.
    """
    GATEWAY_TIMEOUT = 60
    BATCH_SIZE = 50  # Limit number of tasks scheduled at once
    try:
        while True:
            now = time.time()
            for g_id, info in list(gateways_info.items()):
                if now - info["last_seen"] > GATEWAY_TIMEOUT:
                    logger.warning(f"Gateway {g_id} timed out, removing.")

                    devices = list(info["devices"])
                    for i in range(0, len(devices), BATCH_SIZE):
                        batch = devices[i:i + BATCH_SIZE]
                        async with asyncio.TaskGroup() as tg:
                            for dev_uuid in batch:
                                tg.create_task(deregister_asset_async(dev_uuid))
                                tg.create_task(deregister_asset_async(dev_uuid + '-PRODUCER'))
                    del gateways_info[g_id]
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.info("Cleanup task cancelled gracefully")
        # Optionally do final cleanup here

        raise  # Re-raise to properly signal cancellation


# ----------------------------
# Coordinator startup helper: create OPC UA assignment tables
# ----------------------------
async def create_opcua_assignment_tables():
    """ Ensure that the KSQLDB tables for OPC UA gateway assignments exist. """
    logger.info("Creating OPC UA assignment tables if they do not exist.")

    # Source tables
    ksql.statement_query("""
    CREATE TABLE IF NOT EXISTS OPCUA_DEVICE_ASSIGNMENT_SOURCE (
        DEVICE_UUID STRING PRIMARY KEY,
        GATEWAY_ID STRING
    ) WITH (
        KAFKA_TOPIC='opcua_device_assignment_topic',
        VALUE_FORMAT='JSON',
        PARTITIONS=1
    );
    CREATE TABLE IF NOT EXISTS OPCUA_GATEWAYS_SOURCE (
        GATEWAY_ID STRING PRIMARY KEY,
        GATEWAY_HOST STRING
    ) WITH (
        KAFKA_TOPIC='opcua_gateways_topic',
        VALUE_FORMAT='JSON',
        PARTITIONS=1
    );
    """)

    # Materialized tables
    ksql.statement_query("""
    CREATE TABLE IF NOT EXISTS OPCUA_DEVICE_ASSIGNMENT AS
        SELECT DEVICE_UUID, GATEWAY_ID
        FROM OPCUA_DEVICE_ASSIGNMENT_SOURCE
        EMIT CHANGES;
    CREATE TABLE IF NOT EXISTS OPCUA_GATEWAYS AS
        SELECT GATEWAY_ID, GATEWAY_HOST
        FROM OPCUA_GATEWAYS_SOURCE
        EMIT CHANGES;
    """)


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
    # Create OPC UA assignment tables
    await create_opcua_assignment_tables()

    # Coordinator registration
    logger.info("Register OPC UA Coordinator with OpenFactory.")
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
    logger.info("Start Gateways cleanup task.")
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
app = FastAPI(title="OPCUA Coordinator",
              version=os.environ.get('APPLICATION_VERSION'),
              lifespan=lifespan)
ksql = KSQLDBClient(os.getenv("KSQLDB_URL"))
kafka_producer = Producer({'bootstrap.servers': os.getenv("KAFKA_BROKER")})


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
    # Check if gateway already exists
    gateway_uuid = next((k for k, info in gateways_info.items() if info["url"] == gateway_host), None)

    if gateway_uuid:
        gateways_info[gateway_uuid] = {
            "last_seen": time.time(),
            "url": gateway_host,
            "devices": set(req.devices.keys()) if req.devices else gateways_info[gateway_uuid]["devices"],
        }
        logger.debug(f"Refreshed gateway {gateway_uuid} ({gateway_host}) (periodic re-registration)")
    else:
        existing_ids = [
            int(k.replace("OPCUA-GATEWAY-", ""))
            for k in gateways_info.keys()
            if k.startswith("OPCUA-GATEWAY-")
        ]

        # Find first unused ID
        new_index = 1
        existing_ids_set = set(existing_ids)
        while new_index in existing_ids_set:
            new_index += 1

        gateway_uuid = f"OPCUA-GATEWAY-{new_index}"
        gateways_info[gateway_uuid] = {
            "last_seen": time.time(),
            "url": gateway_host,
            "devices": set(req.devices.keys()) if req.devices else set(),
        }

        # Gateway attributes
        coordinator.add_reference_below(gateway_uuid)
        gateway = await create_asset_async(gateway_uuid)
        gateway.add_attribute(AssetAttribute(id='avail', value="AVAILABLE", tag="Availability", type="Events"))
        gateway.add_attribute(AssetAttribute(id='uri', value=gateway_host, tag="GatewayURI", type="Events"))
        gateway.add_attribute(AssetAttribute(id='application_manufacturer', value='OpenFactoryIO', type='Events', tag='Application.Manufacturer'))
        gateway.add_attribute(AssetAttribute(id='application_license', value='Polyform Noncommercial License 1.0.0', type='Events', tag='Application.License'))
        gateway.add_attribute(AssetAttribute(id='application_version', value=os.environ.get('APPLICATION_VERSION'), type='Events', tag='Application.Version'))

        logger.info(f"Registered new gateway {gateway_uuid} ({gateway_host})")

    return {
        "status": "registered",
        "gateway_id": gateway_uuid,
        "device_count": len(gateways_info[gateway_uuid]["devices"]),
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

    # Round-robin assignment based on least devices
    assigned_gateway = min(gateways_info.items(), key=lambda x: len(x[1]["devices"]))[0]
    gateways_info[assigned_gateway]["devices"].add(device_uuid)
    logger.info(f"Device {device_uuid} assigned to {assigned_gateway}")

    async def notify_gateway():
        url = f"{gateways_info[assigned_gateway]["url"]}/add_device"
        payload = {"device": req.device.model_dump()}
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=payload, timeout=5)
                resp.raise_for_status()
                logger.info(f"✅ Gateway notified: {url} response={resp.status_code}")
        except Exception as e:
            logger.error(f"❌ Failed to notify gateway {url}: {e}")

    async def persist_assignment():
        """
        Persists assignment.
        Limit concurrent registrations using a semaphore to prevent thread exhaustion.
        """
        async with thread_semaphore:
            try:
                await asyncio.to_thread(
                    ksql.insert_into_stream,
                    "OPCUA_DEVICE_ASSIGNMENT_SOURCE",
                    [{"DEVICE_UUID": device_uuid, "GATEWAY_ID": assigned_gateway}]
                )
                logger.info(f"Recorded assignment of device {device_uuid} to gateway {assigned_gateway} in KSQLDB")
            except Exception as e:
                logger.error(f"Failed to record assignment of {device_uuid} in KSQLDB: {e}")

    async def register_producer_safe():
        """
        Register Producer.
        Limit concurrent registrations using a semaphore to prevent thread exhaustion.
        """
        async with thread_semaphore:
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

    async with asyncio.TaskGroup() as tg:
        tg.create_task(notify_gateway())
        tg.create_task(register_producer_safe())
        tg.create_task(persist_assignment())

    return {"device_uuid": device_uuid, "assigned_gateway": assigned_gateway}


@app.delete("/unregister_device/{device_uuid}")
async def unregister_device(device_uuid: str):
    """
    Unregister a device from all gateways where it may exist.
    """
    # Find the gateway containing this device
    assigned_gateway = None
    for gw, info in gateways_info.items():
        if device_uuid in info["devices"]:
            assigned_gateway = gw
            info["devices"].remove(device_uuid)
            break

    if not assigned_gateway:
        raise HTTPException(status_code=404, detail=f"Device {device_uuid} not found")

    async def notify_remove():
        """ Deregister device with Gateway. """
        url = f"{gateways_info[assigned_gateway]["url"]}/remove_device/{device_uuid}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.delete(url, timeout=5)
                resp.raise_for_status()
                logger.info(f"✅ Gateway notified to remove {device_uuid}: {url} response={resp.status_code}")
        except Exception as e:
            logger.error(f"❌ Failed to notify {assigned_gateway} for {device_uuid}: {e}")

    async def deregister_device():
        """
        Deregister device and producer with OpenFactory.
        Limit concurrent deregistrations using a semaphore to prevent thread exhaustion.
        """
        async with thread_semaphore:
            try:
                await deregister_asset_async(device_uuid.upper())
                logger.info(f"✅ Device {device_uuid} deregistered")
                await deregister_asset_async(device_uuid.upper() + '-PRODUCER')
                logger.info(f"✅ Producer deregistered for {device_uuid}")
            except Exception as e:
                logger.error(f"❌ Failed to deregister producer for {device_uuid}: {e}")

    async def remove_assignment():
        """
        Remove assignment from KSQLDB (tombstone).
        Limit concurrent deregistrations using a semaphore to prevent thread exhaustion.
        """
        async with thread_semaphore:
            def _send_tombstone():
                kafka_producer.produce('opcua_device_assignment_topic', key=device_uuid, value=None)
                kafka_producer.flush()
                logger.debug(f"Sent tombstone for device {device_uuid} to topic 'opcua_device_assignment_topic'")

            try:
                await asyncio.to_thread(_send_tombstone)
                logger.info(f"✅ Assignment removed from KSQLDB for {device_uuid}")
            except Exception as e:
                logger.error(f"❌ Failed to remove assignment from KSQLDB for {device_uuid}: {e}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(notify_remove())
        tg.create_task(deregister_device())
        tg.create_task(remove_assignment())

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
    return {dev: gw for gw, info in gateways_info.items() for dev in info["devices"]}


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
