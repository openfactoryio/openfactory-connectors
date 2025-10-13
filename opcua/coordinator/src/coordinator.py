import os
import logging
import asyncio
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
from confluent_kafka import Producer

from openfactory.kafka import KSQLDBClient
from openfactory.assets import Asset, AssetAttribute
from openfactory.schemas.devices import Device
from openfactory.utils import register_asset, deregister_asset

from . import coordinator_metrics


# ----------------------------
# Logger setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("opcua-coordinator")


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


async def fetch_devices_count(client, gw_id, gw_host):
    """
    Asynchronously fetch the number of registered devices from a gateway.

    This coroutine queries a gateway's `/devices_count` endpoint using an
    asynchronous HTTP client. It limits concurrent requests via a global
    semaphore to prevent excessive parallel connections. If the gateway
    is unreachable or returns an error, it is treated as "busy" by assigning
    an infinite device count (`float('inf')`).

    Args:
        client (httpx.AsyncClient): The HTTP client instance used for making the request.
        gw_id (str): The unique identifier of the gateway (e.g., "OPCUA-GATEWAY-1").
        gw_host (str): The base URL of the gateway (e.g., "http://172.19.0.6:8001").

    Returns:
        tuple: A tuple containing:
            - The gateway ID (`gw_id`).
            - A dictionary with:
                - "host" (str): The gateway's host URL.
                - "devices_count" (int or float): The number of registered devices,
                  or `float('inf')` if the gateway is unreachable.
    """
    async with thread_semaphore:
        url = f"{gw_host}/devices_count"
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            count = resp.json()
            return gw_id, {"host": gw_host, "devices_count": count}
        except Exception as e:
            logger.warning(f"Failed to get devices_count from {gw_id} ({gw_host}): {e}")
            # Treat as "busy" if unreachable
            return gw_id, {"host": gw_host, "devices_count": float('inf')}


async def update_gateway_metrics_periodically(interval: float = 10.0):
    """
    Periodically update Prometheus metrics.

    Args:
        interval (float): Time in seconds between updates.
    """
    while True:
        # Query deployed gateways from KSQL
        try:
            gateways_rows = ksql.query("SELECT GATEWAY_ID, GATEWAY_HOST FROM OPCUA_GATEWAYS;")
        except Exception as e:
            logger.error(f"Failed to query OPCUA_GATEWAYS table: {e}")
            gateways_rows = []

        gateways = {row["GATEWAY_ID"]: row["GATEWAY_HOST"] for row in gateways_rows}

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                tasks = [fetch_devices_count(client, gw_id, gw_host) for gw_id, gw_host in gateways.items()]
                responses = await asyncio.gather(*tasks)
            results = dict(responses)

            # Update Prometheus metrics
            coordinator_metrics.update_gateway_metrics(results)
        except Exception as e:
            logger.warning(f"Failed to update gateway metrics: {e}")

        await asyncio.sleep(interval)


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


async def get_assigned_gateway_url(device_uuid: str) -> tuple[str, str] | None:
    """
    Return the Gateway host to which a device is assigned.

    Args:
        device_uuid (str): Device UUID to look up.

    Returns:
        tuple: Gateway host and ID to which a device is assigned.
    """
    rows = ksql.query(
        f"SELECT GATEWAY_ID FROM OPCUA_DEVICE_ASSIGNMENT WHERE DEVICE_UUID='{device_uuid}';"
        )
    if not rows:
        return None

    gateway_id = rows[0]["GATEWAY_ID"]
    rows = ksql.query(f"SELECT GATEWAY_HOST FROM OPCUA_GATEWAYS WHERE GATEWAY_ID='{gateway_id}';")
    if not rows:
        return None

    gateway_host = rows[0]["GATEWAY_HOST"]
    return gateway_host, gateway_id


async def assign_gateway_id() -> str:
    """
    Assign a gateway ID for a new registration.

    The function tries to reuse an existing gateway ID from gateways that are currently down
    (based on their /status endpoint). If all known gateways are up, it generates a new gateway ID.

    Returns:
        str: A gateway ID, either reused or newly generated.
    """
    # Query deployed gateways from KSQL
    try:
        gateways_rows = ksql.query("SELECT GATEWAY_ID, GATEWAY_HOST FROM OPCUA_GATEWAYS;")
    except Exception as e:
        logger.error(f"Failed to query OPCUA_GATEWAYS table: {e}")
        gateways_rows = []

    gateways = {row["GATEWAY_ID"]: row["GATEWAY_HOST"] for row in gateways_rows}

    # Check /status for gateway
    async def check_status(g_id: str, url: str) -> tuple[str, bool]:
        status_url = f"{url}/status"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(status_url)
                resp.raise_for_status()
                data = resp.json()
                return g_id, data.get("status") == "AVAILABLE"
        except httpx.RequestError as e:
            logger.debug(f"Gateway {g_id} ({url}) not reachable: {e}")
            return g_id, False
        except Exception as e:
            logger.error(f"Unexpected error while checking gateway {g_id} ({url}): {e}", exc_info=True)
            return g_id, False

    tasks = [check_status(g_id, url) for g_id, url in gateways.items()]
    results = await asyncio.gather(*tasks)

    # Try to find an UNAVAILABLE gateway
    for g_id, is_available in results:
        if not is_available:
            logger.debug(f"Assigning gateway ID {g_id}")
            return g_id

    # No UNAVAILABLE gateway found, generate a new ID
    existing_ids = [
        int(g_id.replace("OPCUA-GATEWAY-", ""))
        for g_id in gateways.keys()
        if g_id.startswith("OPCUA-GATEWAY-")
    ]
    new_index = 1
    existing_ids_set = set(existing_ids)
    while new_index in existing_ids_set:
        new_index += 1

    new_id = f"OPCUA-GATEWAY-{new_index}"
    logger.debug(f"No unavailable gateway found, generated new ID {new_id}")
    return new_id


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
    logger.info("Start Coordinator Prometheus metrics task.")
    metrics_task = asyncio.create_task(update_gateway_metrics_periodically())

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
    metrics_task.cancel()
    try:
        await metrics_task
    except asyncio.CancelledError:
        logger.info("Coordinator Prometheus metrics task cancelled on shutdown")


# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="OPCUA Coordinator",
              version=os.environ.get('APPLICATION_VERSION'),
              lifespan=lifespan)

# Expose Prometheus metrics
app.get("/metrics")(coordinator_metrics.metrics_endpoint)

ksql = KSQLDBClient(os.getenv("KSQLDB_URL"))
kafka_producer = Producer({'bootstrap.servers': os.getenv("KAFKA_BROKER")})
gateway_register_lock = asyncio.Lock()


# ----------------------------
# Pydantic models
# ----------------------------
class RegisterGatewayRequest(BaseModel):
    gateway_id: str
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
        req (RegisterGatewayRequest): Gateway registration payload.

    Returns:
        dict: A confirmation dictionary with keys:
            - "status" (str): Registration status.
            - "gateway_id" (str): The assigned gateway ID.
    """
    async with gateway_register_lock:
        gateway_host = req.gateway_host

        if req.gateway_id == 'UNAVAILABLE':

            logger.info("Assigning Gateway ID")
            gateway_id = await assign_gateway_id()
            logger.info(f"Assigned {gateway_id}")

            # Register Gateway in KSQLDB
            try:
                sql = f"""
                INSERT INTO OPCUA_GATEWAYS_SOURCE (GATEWAY_ID, GATEWAY_HOST)
                VALUES ('{gateway_id}', '{gateway_host}');
                """
                ksql.statement_query(sql)
                logger.info(f"Registered gateway {gateway_id} in KSQLDB with host {gateway_host}")
            except Exception as e:
                logger.error(f"Failed to register gateway {gateway_id} in KSQLDB: {e}")

            # Register Gateway attributes
            coordinator.add_reference_below(gateway_id)
            gateway = await create_asset_async(gateway_id)
            gateway.add_attribute(AssetAttribute(id='avail', value="AVAILABLE", tag="Availability", type="Events"))
            gateway.add_attribute(AssetAttribute(id='uri', value=gateway_host, tag="GatewayURI", type="Events"))
            gateway.add_attribute(AssetAttribute(id='application_manufacturer', value='OpenFactoryIO', type='Events', tag='Application.Manufacturer'))
            gateway.add_attribute(AssetAttribute(id='application_license', value='Polyform Noncommercial License 1.0.0', type='Events', tag='Application.License'))
            gateway.add_attribute(AssetAttribute(id='application_version', value=os.environ.get('APPLICATION_VERSION'), type='Events', tag='Application.Version'))

            logger.info(f"✅ Registered new gateway {gateway_id} ({gateway_host})")

        else:
            gateway_id = req.gateway_id

        return {
            "status": "registered",
            "gateway_id": gateway_id,
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
    with coordinator_metrics.DEVICE_ASSIGNMENT_LATENCY.time():
        device_uuid = req.device.uuid

        # Query deployed gateways from KSQL
        try:
            gateways_rows = ksql.query("SELECT GATEWAY_ID, GATEWAY_HOST FROM OPCUA_GATEWAYS;")
        except Exception as e:
            logger.error(f"Failed to query OPCUA_GATEWAYS table: {e}")
            gateways_rows = []

        gateways = {row["GATEWAY_ID"]: row["GATEWAY_HOST"] for row in gateways_rows}

        if not gateways:
            raise HTTPException(status_code=500, detail="No gateways registered")

        # Query each gateway’s /devices_count endpoint concurrently
        async with httpx.AsyncClient(timeout=5.0) as client:
            tasks = [fetch_devices_count(client, gw_id, gw_host) for gw_id, gw_host in gateways.items()]
            responses = await asyncio.gather(*tasks)
        results = dict(responses)

        # Pick the gateway with the fewest registered devices
        assigned_gateway_id, assigned_info = min(
            results.items(), key=lambda x: x[1]["devices_count"]
        )
        assigned_gateway_host = assigned_info["host"]
        logger.info(f"Device {device_uuid} assigned to {assigned_gateway_id} ({assigned_gateway_host})")

        # Update metrics
        coordinator_metrics.increment_assignment_counter()

        async def notify_gateway():
            url = f"{assigned_gateway_host}/add_device"
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
                        [{"DEVICE_UUID": device_uuid, "GATEWAY_ID": assigned_gateway_id}]
                    )
                    logger.info(f"Recorded assignment of device {device_uuid} to gateway {assigned_gateway_id} in KSQLDB")
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
                        value=assigned_gateway_id,
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

        return {"device_uuid": device_uuid, "assigned_gateway": assigned_gateway_id}


@app.delete("/unregister_device/{device_uuid}")
async def unregister_device(device_uuid: str):
    """
    Unregister a device from all gateways where it may exist.
    """
    # Find the gateway containing this device
    result = await get_assigned_gateway_url(device_uuid)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Device {device_uuid} not found")

    gateway_host, gateway_id = result

    async def notify_remove():
        """ Deregister device with Gateway. """
        url = f"{gateway_host}/remove_device/{device_uuid}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.delete(url, timeout=5)
                resp.raise_for_status()
                logger.info(f"✅ Gateway notified to remove {device_uuid}: {url} response={resp.status_code}")
        except Exception as e:
            logger.error(f"❌ Failed to remove {device_uuid} on {gateway_host}: {e}")

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

    logger.info(f"Device {device_uuid} unregistered locally from {gateway_id} ({gateway_host})")

    return {
        "status": "unregistered",
        "device_uuid": device_uuid,
        "gateway": gateway_id
    }


@app.get("/assignments")
async def get_assignments():
    """
    Retrieve all device-to-gateway assignments.

    Returns:
        Dict[str, str]: A dictionary mapping device UUIDs to assigned gateway hosts.
    """
    rows = ksql.query("SELECT DEVICE_UUID, GATEWAY_ID FROM OPCUA_DEVICE_ASSIGNMENT;")
    gateways = [
        {
            "device_uuid": row["DEVICE_UUID"],
            "gateway_id": row["GATEWAY_ID"]
        }
        for row in rows
    ]
    return gateways


@app.get("/gateways")
async def get_gateways():
    """
    Retrieve the list of all registered gateways.

    Returns:
        List[Dict]: A list of gateway dictionaries, each containing:
            - gateway_id (str): The unique identifier of the gateway.
            - gateway_host (str): The host URL where the gateway is accessible.

    """
    rows = ksql.query("SELECT GATEWAY_ID, GATEWAY_HOST FROM OPCUA_GATEWAYS;")
    gateways = [
        {
            "gateway_id": row["GATEWAY_ID"],
            "gateway_host": row["GATEWAY_HOST"]
        }
        for row in rows
    ]
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
