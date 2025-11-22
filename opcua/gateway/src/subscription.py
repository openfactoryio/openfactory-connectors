"""
OPC UA subscription handler module for the Gateway.

This module defines the `SubscriptionHandler` class, which processes
data change and event notifications from OPC UA servers. Notifications
are enriched with metadata, classified as 'Samples', 'Events', or
'Condition', and forwarded to the global Kafka producer (`global_producer`).

Key components:
- `SubscriptionHandler`: Handles both data changes and event notifications
  for a specific OPC UA device UUID.
"""

from fastapi import FastAPI
from numbers import Number
from typing import Any
from asyncua import ua, Client
from asyncua.common.node import Node
from openfactory.assets import AssetAttribute
from openfactory.assets.utils import openfactory_timestamp, current_timestamp
from .utils import opcua_data_timestamp, opcua_event_timestamp


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
    def __init__(self, opcua_device_uuid: str, app: FastAPI, opcua_client: Client):
        """
        Initialize the SubscriptionHandler.

        Args:
            opcua_device_uuid (str): UUID of the device.
            app (FastAPI): The FastAPI application instance used to access shared state

        Attributes:
            node_map (Dict[Node, Dict[str, str]]): Cache mapping OPC UA Node objects to metadata.
        """
        self.opcua_device_uuid = opcua_device_uuid
        self.logger = app.state.logger
        self.gateway_id = app.state.gateway_id
        self.queue = app.state.queue
        self.client = opcua_client

        # Cache mapping: Node -> {"local_name": str, "browse_name": str}
        self.node_map: dict = {}

    def normalize_value(self, val: object) -> Any:
        """
        Convert OPC UA values into clean Python-native ones.

        Args:
            val (object): The new value of the variable.

        Returns:
            normalized value (Any)
        """
        # LocalizedText → return its text
        if isinstance(val, ua.LocalizedText):
            return val.Text

        # ByteString → return hex string instead of raw bytes
        if isinstance(val, (bytes, bytearray)):
            return list(val)

        # Arrays → normalize each element
        if isinstance(val, list):
            return [self.normalize_value(v) for v in val]

        # ua.Variant: unwrap
        if isinstance(val, ua.Variant):
            return self.normalize_value(val.Value)

        # fallback: return as-is
        return val

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
        """

        # Extract variable name
        info = self.node_map.get(node, {})
        local_name = info.get("local_name", "<unknown>")
        tag = info.get("tag", "<unknown>")
        deadband = info.get("deadband", 0)

        # normalize value
        val = self.normalize_value(val)

        # Determine OpenFactory type based on value type
        ofa_type = "Samples" if isinstance(val, Number) else "Events"

        # Extract DataValue
        data_value: ua.DataValue = data.monitored_item.Value

        # check status
        if not data_value.StatusCode.is_good():
            self.logger.warning(
                f"Received bad or uncertain value for {local_name} ({tag}): StatusCode={data_value.StatusCode}"
            )
            val = "UNAVAILABLE"

        # verify deadband
        if ofa_type == "Samples" and val != "UNAVAILABLE":
            last_val = info.get("last_val", None)
            if last_val is not None and abs(val - float(last_val)) <= deadband:
                return
            self.node_map[node]["last_val"] = val

        self.logger.debug(f"DataChange: {local_name}:({tag}) -> {val}")

        device_timestamp = opcua_data_timestamp(data.monitored_item.Value)
        message = {
            "asset_uuid": self.opcua_device_uuid,
            "asset_attribute": AssetAttribute(
                id=local_name,
                value=val,
                type=ofa_type,
                tag=tag,
                timestamp=openfactory_timestamp(device_timestamp),
            ),
            "device_timestamp": device_timestamp,
            "ingestion_timestamp": current_timestamp(),
        }

        try:
            await self.queue.put(message)
        except Exception as e:
            self.logger.error(f"Failed to enqueue data for {self.opcua_device_uuid}: {e}")
            return

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
            source_name = getattr(event, "SourceName", "opcua")
            event_type = getattr(event, "EventType", None)
            tag = "OPCUAEvent"
            if event_type:
                event_type_node = self.client.get_node(event_type)
                browse_name = await event_type_node.read_browse_name()
                tag = browse_name.Name

            if message and hasattr(message, "Text"):
                message_text = message.Text
            else:
                message_text = str(message)

            if severity:
                message_text = f"{message_text} (Severity: {severity})"

            self.logger.debug(f"Event from {source_name}: {message_text}")

        except Exception as e:
            self.logger.error(f"Error parsing event: {e}, raw event={event}")
            message_text = "UNAVAILABLE"

        device_timestamp = opcua_event_timestamp(event)
        send_payload = {
            "asset_uuid": self.opcua_device_uuid,
            "asset_attribute": AssetAttribute(
                id=f"{source_name}_event",
                value=message_text,
                type="Condition",
                tag=tag,
                timestamp=openfactory_timestamp(device_timestamp),
            ),
            "device_timestamp": device_timestamp,
            "ingestion_timestamp": current_timestamp(),
        }

        try:
            await self.queue.put(send_payload)
        except Exception as e:
            self.logger.error(f"Failed to enqueue event for {self.opcua_device_uuid}: {e}")
            return
