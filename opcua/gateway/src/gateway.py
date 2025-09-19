# python -m opcua.gateway.src.gateway

"""
OPC UA Gateway (multi-device) prototype.

- POST /add_device  -> Device (openfactory.schemas.devices.Device) is registered and a monitor task starts.
- DELETE /remove_device/{uuid} -> stops the monitor task and cleans up the device.
- GET /devices -> lists registered devices and their status (simple view).

Each device uses the same monitoring logic as your single-device OPCUAProducer:
- resolves namespace and device node
- subscribes to variables and events
- uses SubscriptionHandler to translate notifications into Asset attributes
- marks availability and reconnects on errors
"""

import os
import asyncio
import logging
import traceback
import json
from numbers import Number
from typing import Dict, Any
from pydantic import BaseModel
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from asyncua import Client, ua
from asyncua.common.node import Node

from openfactory.schemas.devices import Device
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema
from openfactory.assets import AssetAttribute
from openfactory.assets.utils import openfactory_timestamp, current_timestamp
from openfactory.kafka import KSQLDBClient


# ----------------------------
# Global producer singleton
# ----------------------------
class GlobalAssetProducer(Producer):
    """
    Single Kafka producer shared by all OPC UA devices.

    This producer sends asset attributes from all devices to the ASSETS_STREAM topic.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, ksqlClient: KSQLDBClient, bootstrap_servers: str = None):
        if hasattr(self, "_initialized"):
            return  # avoid re-initializing singleton
        bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BROKER")
        super().__init__({'bootstrap.servers': bootstrap_servers})
        self.ksql = ksqlClient
        self.topic = self.ksql.get_kafka_topic("ASSETS_STREAM")
        self._initialized = True

    def send(self, asset_uuid: str, asset_attribute: AssetAttribute) -> None:
        """
        Send a Kafka message for the given asset and attribute.

        Args:
            asset_uuid (str): UUID of the asset.
            asset_attribute (AssetAttribute): Attribute data to send.
        """
        msg = {
            "ID": asset_attribute.id,
            "VALUE": asset_attribute.value,
            "TAG": asset_attribute.tag,
            "TYPE": asset_attribute.type,
            "attributes": {"timestamp": asset_attribute.timestamp}
        }
        self.produce(topic=self.topic, key=asset_uuid, value=json.dumps(msg))
        # optionally flush here, or batch periodically


# ----------------------------
# Logging
# ----------------------------
logger = logging.getLogger("opcua.gateway")
log_level = os.getenv("OPCUA_GATEWAY_LOG_LEVEL", "DEBUG").upper()
logger.setLevel(log_level)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    logger.addHandler(ch)


# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="OPCUA Gateway", version="0.1")

# Active monitor tasks and assets
_active_tasks: Dict[str, asyncio.Task] = {}
_active_device_defs: Dict[str, Device] = {}

# Global producer instance
global_producer = GlobalAssetProducer(KSQLDBClient(os.getenv("KSQLDB_URL")))


# ----------------------------
# Helper timestamp extractors
# ----------------------------
def opcua_data_timestamp(data: ua.DataValue) -> str:
    """
    Extract the most relevant timestamp from an OPC UA DataValue.

    Priority order:
        1. SourceTimestamp (preferred, device time)
        2. ServerTimestamp (fallback, server processing time)
        3. Current system time (last resort)

    Args:
        data (ua.DataValue): The OPC UA DataValue to extract timestamps from.

    Returns:
        str: OpenFactory-formatted timestamp string.
    """
    if data.SourceTimestamp:
        return openfactory_timestamp(data.SourceTimestamp)
    if data.ServerTimestamp:
        return openfactory_timestamp(data.ServerTimestamp)
    return current_timestamp()


def opcua_event_timestamp(event: Any) -> str:
    """
    Extract the most relevant timestamp from an OPC UA event.

    Priority order:
        1. event.Time
        2. event.ReceiveTime
        3. Current UTC time

    Args:
        event (Any): OPC UA event object.

    Returns:
        str: OpenFactory-formatted timestamp string.
    """
    event_time = getattr(event, "Time", None)
    receive_time = getattr(event, "ReceiveTime", None)

    if isinstance(event_time, type(event_time)) and hasattr(event_time, "isoformat"):
        return openfactory_timestamp(event_time)
    if isinstance(receive_time, type(receive_time)) and hasattr(receive_time, "isoformat"):
        return openfactory_timestamp(receive_time)
    return current_timestamp()


# ----------------------------
# SubscriptionHandler
# ----------------------------
class SubscriptionHandler:
    """
    Handle OPC UA event notifications.

    This callback is invoked when the client receives an event notification
    from the server (e.g., alarms, conditions, or system events). It extracts
    key fields such as message, severity, active state, source, and timestamp,
    and forwards them as an OpenFactory attribute.

    Args:
        event (Any): The OPC UA event object delivered by the subscription.

    Returns:
        None
    """
    def __init__(self, opcua_device_uuid: str, logger: logging.Logger):
        """
        Initialize the SubscriptionHandler.

        Args:
            opcua_device_uuid (str): UUID of the device.
            logger (logging.Logger): Logger instance for debug and error messages.

        Attributes:
            node_map (Dict[Node, Dict[str, str]]): Cache mapping OPC UA Node objects to metadata.
        """
        self.opcua_device_uuid = opcua_device_uuid
        self.logger = logger

        # Cache mapping: Node -> {"local_name": str, "browse_name": str}
        self.node_map: dict = {}

    async def datachange_notification(self, node: Node, val: object, data: ua.DataChangeNotification) -> None:
        """
        Handle OPC UA data change notifications.

        This method is called by the OPC UA client subscription handler whenever
        a monitored variable's value changes. It enriches the notification with
        metadata (local name, browse name, and timestamp) and forwards the update
        into the OpenFactory pipeline.

        Numeric values are sent as 'Samples', non-numeric as 'Events'.
        Values with bad or uncertain StatusCode are reported as 'UNAVAILABLE'.

        Args:
            node (Node): The OPC UA node that triggered the data change.
            val (object): The new value of the variable.
            data (ua.DataChangeNotification): The notification containing the
                DataValue, including timestamps and status information.

        Returns:
            None
        """

        # Extract variable name
        info = self.node_map.get(node, {})
        local_name = info.get("local_name", "<unknown>")
        browse_name = info.get("browse_name", "<unknown>")

        # Extract DataValue
        data_value: ua.DataValue = data.monitored_item.Value

        # Determine OpenFactory type based on value type
        ofa_type = "Samples" if isinstance(val, Number) else "Events"

        # check status
        if not data_value.StatusCode.is_good():
            self.logger.warning(
                f"Received bad or uncertain value for {local_name} ({browse_name}): StatusCode={data_value.StatusCode}"
            )
            val = "UNAVAILABLE"

        self.logger.debug(f"DataChange: {local_name}:({browse_name}) -> {val}")
        print(f"DataChange: {local_name}:({browse_name}) -> {val}")

        global_producer.send(
            asset_uuid=self.opcua_device_uuid,
            asset_attribute=AssetAttribute(
                id=local_name,
                value=val,
                type=ofa_type,
                tag=browse_name,
                timestamp=opcua_data_timestamp(data.monitored_item.Value),
            )
        )

    async def event_notification(self, event: Any) -> None:
        """
        Handle OPC UA event notifications.

        This callback is invoked when the client receives an event notification
        from the server (e.g., alarms, conditions, or system events). It extracts
        key fields such as message, severity, active state, source, and timestamp,
        and forwards them as an OpenFactory attribute.

        Args:
            event (Any): The OPC UA event object delivered by the subscription.

        Returns:
            None
        """
        try:
            message = getattr(event, "Message", None)
            severity = getattr(event, "Severity", None)
            active = getattr(event, "ActiveState", None)
            source = getattr(event, "SourceName", None)

            if message and hasattr(message, "Text"):
                message_text = message.Text
            else:
                message_text = str(message)

            if severity:
                message_text = f"{message_text} (Severity: {severity})"

            # Determine Condition tag based on active state
            tag = "Fault"
            if active and hasattr(active, "EffectiveDisplayName"):
                if active.EffectiveDisplayName != "Active":
                    tag = "Normal"

            self.logger.debug(f"Event from {source} (severity {severity}): {message_text}")

        except Exception as e:
            self.logger.error(f"Error parsing event: {e}, raw event={event}")
            message_text = "UNAVAILABLE"

        global_producer.send(
            asset_uuid=self.opcua_device_uuid,
            asset_attribute=AssetAttribute(
                id="alarm",
                value=message_text,
                type="Condition",
                tag=tag,
                timestamp=opcua_event_timestamp(event),
            )
        )


class DeviceMonitor:
    """
    Manage the lifecycle of an OPC UA monitored device.

    Each device gets its own monitor instance, which handles:
    - Parsing connector schema
    - Establishing OPC UA sessions
    - Subscribing to variables/events
    - Updating asset attributes in Kafka/ksqlDB
    - Handling reconnects and cleanup
    """

    def __init__(self, device: Device, logger: logging.Logger):
        """
        Initialize the monitor for a given device.

        Args:
            device (Device): The device definition, including UUID and connector schema.
        """
        self.device = device
        self.dev_uuid = device.uuid
        self.schema = OPCUAConnectorSchema(**device.connector.model_dump())
        self.sub = None

        # Register device globally
        _active_device_defs[self.dev_uuid] = self.device

        # logging
        self.log = logger

    async def run(self) -> None:
        """
        Main entrypoint for monitoring loop.

        Keeps trying to connect and monitor the device until cancelled.
        Reconnects automatically on errors with a small backoff.
        """
        self.log.info(f"Starting monitor task for {self.dev_uuid}")
        while True:
            try:
                await self._run_session()
            except asyncio.CancelledError:
                await self._cleanup()
                raise
            except Exception as e:
                self.log.error(f"[{self.dev_uuid}] OPC UA client error: {type(e).__name__}: {e}")
                self.log.debug(traceback.format_exc())
                try:
                    global_producer.send(
                        asset_uuid=self.dev_uuid,
                        asset_attribute=AssetAttribute(id='avail', value="UNAVAILABLE", tag="Availability", type="Events")
                    )
                except Exception:
                    pass
                await asyncio.sleep(2)  # backoff before reconnect

    async def _run_session(self) -> None:
        """
        Establish a session and subscribe to variables/events.

        Runs until the connection is lost. A reconnect is triggered
        by exceptions from the keepalive loop.
        """
        async with Client(self.schema.server.uri) as client:
            idx = await self._resolve_namespace(client)
            device_node = await self._resolve_device_node(client, idx)

            handler = SubscriptionHandler(self.dev_uuid, self.log)
            self.sub = await client.create_subscription(
                period=float(self.schema.server.subscription.publishing_interval),
                handler=handler,
            )

            await self._subscribe_variables(device_node, idx, handler)
            await self._subscribe_events(device_node)

            global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=AssetAttribute(id='avail', value="AVAILABLE", tag="Availability", type="Events")
            )
            self.log.info(f"[{self.dev_uuid}] Connected to OPC UA server at {self.schema.server.uri}")

            await self._keepalive_loop(device_node)

    async def _resolve_namespace(self, client: Client) -> int:
        """
        Resolve the namespace index from the server URI.

        Args:
            client (Client): Connected OPC UA client.

        Returns:
            int: The namespace index.

        Raises:
            Exception: If namespace resolution fails.
        """
        try:
            return await client.get_namespace_index(self.schema.server.namespace_uri)
        except Exception as e:
            self.log.error(f"Failed to resolve namespace URI {self.schema.server.namespace_uri}: {e}")
            raise

    async def _resolve_device_node(self, client: Client, idx: int):
        """
        Resolve the device node from path or node ID.

        Args:
            client (Client): Connected OPC UA client.
            idx (int): Namespace index.

        Returns:
            Node: The resolved OPC UA node.

        Raises:
            Exception: If resolution fails.
        """
        try:
            if self.schema.device.path:
                path_parts = [f"{idx}:{p}" for p in self.schema.device.path.split("/")]
                return await client.nodes.objects.get_child(path_parts)
            return client.get_node(self.schema.device.node_id)
        except Exception as e:
            self.log.error(
                f"Failed to resolve device node (path={self.schema.device.path} node_id={self.schema.device.node_id}): {e}"
            )
            raise

    async def _subscribe_variables(self, device_node, idx: int, handler: SubscriptionHandler) -> None:
        """
        Subscribe to all configured variables.

        Args:
            device_node (Node): The resolved device node.
            idx (int): Namespace index.
            handler (SubscriptionHandler): Handles variable updates.
        """
        if not self.schema.device.variables:
            return
        for local_name, var_cfg in self.schema.device.variables.items():
            try:
                var_node = await device_node.get_child([f"{idx}:{var_cfg.browse_name}"])
                await self.sub.subscribe_data_change(
                    var_node,
                    queuesize=var_cfg.queue_size,
                    sampling_interval=var_cfg.sampling_interval,
                )
                qname = await var_node.read_browse_name()
                handler.node_map[var_node] = {"local_name": local_name, "browse_name": qname.Name}
                self.log.info(f"[{self.dev_uuid}] Subscribed variable {local_name} ({var_cfg.browse_name})")
            except Exception as e:
                self.log.error(f"[{self.dev_uuid}] Failed to subscribe variable {local_name} ({var_cfg.browse_name}): {e}")

    async def _subscribe_events(self, device_node) -> None:
        """
        Subscribe to events on the device node.

        Args:
            device_node (Node): The resolved device node.
        """
        try:
            await self.sub.subscribe_events(device_node)
            self.log.info(f"[{self.dev_uuid}] Subscribed to events on device")
        except Exception as e:
            self.log.error(f"[{self.dev_uuid}] Failed to subscribe to events: {e}")

    async def _keepalive_loop(self, device_node) -> None:
        """
        Keep the session alive by polling the device node.

        Args:
            device_node (Node): The resolved device node.

        Raises:
            Exception: If the server becomes unreachable.
        """
        while True:
            await asyncio.sleep(1)
            await device_node.read_display_name()  # exception triggers reconnect

    async def _cleanup(self) -> None:
        """
        Cleanup resources when monitor task is cancelled.

        Ensures asset availability is set to UNAVAILABLE, deletes the
        subscription, and removes device from global registries.
        """
        self.log.info(f"[{self.dev_uuid}] Monitor task cancelled; cleaning up")
        try:
            global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=AssetAttribute(id='avail', value="UNAVAILABLE", tag="Availability", type="Events")
            )
        except Exception:
            pass
        try:
            if self.sub:
                await self.sub.delete()
        except Exception:
            pass
        _active_device_defs.pop(self.dev_uuid, None)


# ----------------------------
# Per-device monitor (core)
# ----------------------------
async def monitor_device(device: Device) -> None:
    """
    Run subscription loop for a single device.

    This function is cancellable. When cancelled it will attempt a clean shutdown.

    Args:
        device (Device): Device schema instance defining OPC UA connector settings.

    Returns:
        None
    """
    monitor = DeviceMonitor(device, logger)
    await monitor.run()


# ----------------------------
# API endpoints
# ----------------------------
class AddDeviceRequest(BaseModel):
    device: Device


@app.post("/add_device")
async def add_device(req: AddDeviceRequest):
    """
    Register device and start monitoring task.

    Args:
        req (AddDeviceRequest): Request containing a `Device` schema.

    Returns:
        dict: Status and registered device UUID.
    """
    device = req.device
    if device.uuid in _active_tasks:
        raise HTTPException(status_code=400, detail=f"Device {device.uuid} already added")

    # create monitor task
    task = asyncio.create_task(monitor_device(device))
    _active_tasks[device.uuid] = task

    logger.info(f"Device {device.uuid} registered, monitor task started")
    return {"status": "started", "device_uuid": device.uuid}


@app.delete("/remove_device/{device_uuid}")
async def remove_device(device_uuid: str):
    """
    Stop monitoring and remove a device.

    Args:
        device_uuid (str): UUID of the device to remove.

    Returns:
        dict: Status and removed device UUID.
    """
    task = _active_tasks.pop(device_uuid, None)
    _active_device_defs.pop(device_uuid, None)

    if not task:
        raise HTTPException(status_code=404, detail="Device not found")

    # cancel and wait for task to finish cleaning up
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    logger.info(f"Device {device_uuid} stopped and removed")
    return {"status": "removed", "device_uuid": device_uuid}


@app.get("/devices")
async def list_devices():
    """
    List all registered devices with a status summary.

    Returns:
        dict: Mapping of device UUIDs to connector info and task status.
    """
    out = {}
    for uuid, dev in _active_device_defs.items():
        out[uuid] = {
            "uuid": uuid,
            "connector": getattr(dev, "connector", None),
            "task_running": (not _active_tasks[uuid].done()) if uuid in _active_tasks else False,
        }
    return out


# ----------------------------
# Entry point for local dev
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("opcua.gateway.src.gateway:app", host="0.0.0.0", port=int(os.getenv("OPCUA_GATEWAY_PORT", 8001)), reload=True)
