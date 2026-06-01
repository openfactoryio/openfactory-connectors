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

from __future__ import annotations
from numbers import Number
from typing import Any, TYPE_CHECKING
from asyncua import ua, Client
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from openfactory.assets import AssetAttribute
from openfactory.assets.utils import openfactory_timestamp, current_timestamp
from .utils import opcua_data_timestamp, opcua_event_timestamp, normalize_value

if TYPE_CHECKING:
    from .main import OPCUAGateway


class SubscriptionHandler:
    """
    Handle OPC UA subscription notifications.

    Processes both data change and event notifications received from OPC UA
    subscriptions, enriches them with metadata, and forwards them to the
    OpenFactory ingestion pipeline.
    """

    def __init__(self, opcua_device_uuid: str, app: OPCUAGateway, opcua_client: Client):
        """
        Initialize the SubscriptionHandler.

        Args:
            opcua_device_uuid (str): UUID of the device.
            app (OPCUAGateway): The OPCUAGateway instance used to access shared state
            opcua_client (Client): OPC UA client used to resolve event type metadata.

        Attributes:
            node_map (dict): Cache mapping OPC UA nodes to subscription metadata,
                including the OpenFactory attribute ID, tag name, deadband
                configuration, and last transmitted value.
        """
        self.opcua_device_uuid = opcua_device_uuid
        self.logger = app.logger
        self.gateway_id = app.asset_uuid
        self.global_producer = app.global_producer
        self.client = opcua_client

        # Cache mapping: Node -> {"local_name", "tag", "deadband", "last_val"}
        self.node_map: dict = {}

    async def datachange_notification(self, node: Node, val: object, data: DataChangeNotif) -> None:
        """
        Handle OPC UA data change notifications.

        This method is called by the OPC UA client subscription handler whenever
        a monitored variable's value changes. It enriches the notification with
        metadata and timestamp and forwards the update into the OpenFactory pipeline.

        Numeric values are sent as 'Samples', non-numeric as 'Events'.
        Numeric values are suppressed if the change does not exceed the configured deadband.
        Values with bad or uncertain StatusCode are reported as 'UNAVAILABLE'.

        Args:
            node (Node): The OPC UA node that triggered the data change.
            val (object): The new value of the variable.
            data (DataChangeNotif): Subscription notification wrapper
                containing the monitored item, DataValue, timestamps,
                and status information.
        """

        # Extract variable name
        info = self.node_map.get(node, {})
        local_name = info.get("local_name", "<unknown>")
        tag = info.get("tag", "<unknown>")
        deadband = info.get("deadband", 0)

        # normalize value
        val = normalize_value(val)

        # Determine OpenFactory type based on value type
        ofa_type = "Samples" if isinstance(val, Number) else "Events"
        # Case for Boolean variables (they will appear as 'true'/'false' in Kafka but Python consider them as numbers)
        if isinstance(val, bool):
            ofa_type = 'Events'

        # Extract DataValue
        data_value: ua.DataValue = data.monitored_item.Value

        # check status
        if not data_value.StatusCode.is_good():
            self.logger.warning(f"Received bad / uncertain value for {local_name} ({tag}): StatusCode={data_value.StatusCode}")
            val = "UNAVAILABLE"

        # verify deadband
        if ofa_type == "Samples" and val != "UNAVAILABLE":
            last_val = info.get("last_val", None)
            if last_val is not None and abs(val - float(last_val)) <= deadband:
                return
            self.node_map[node]["last_val"] = val

        self.logger.debug(f"DataChange: {local_name}:({tag}) -> {val}")

        device_timestamp = opcua_data_timestamp(data.monitored_item.Value)
        attribute = AssetAttribute(
            id=local_name,
            value=val,
            type=ofa_type,
            tag=tag,
            timestamp=openfactory_timestamp(device_timestamp),
            )

        try:
            self.global_producer.send(
                asset_uuid=self.opcua_device_uuid,
                asset_attribute=attribute,
                ingestion_timestamp=current_timestamp()
            )
            self.logger.debug(f"Sent data to Kafka {str(attribute)}")
        except Exception as e:
            self.logger.error(f"Failed to send data to Kafka for {self.opcua_device_uuid}: {e}")
            return

    async def event_notification(self, event: Any) -> None:
        """
        Handle OPC UA event notifications.

        This callback is invoked when the client receives an event notification
        from the server (e.g., alarms, conditions, or system events). It extracts
        key fields such as message, severity, source, and timestamp,
        and forwards them as an OpenFactory attribute.

        Args:
            event (Any): The OPC UA event object delivered by the subscription.
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
        attribute = AssetAttribute(
            id=f"{source_name}_event",
            value=message_text,
            type="Condition",
            tag=tag,
            timestamp=openfactory_timestamp(device_timestamp),
            )

        try:
            self.global_producer.send(
                asset_uuid=self.opcua_device_uuid,
                asset_attribute=attribute,
                ingestion_timestamp=current_timestamp()
            )
            self.logger.debug(f"Sent data to Kafka {str(attribute)}")
        except Exception as e:
            self.logger.error(f"Failed to send event to Kafka for {self.opcua_device_uuid}: {e}")
            return
