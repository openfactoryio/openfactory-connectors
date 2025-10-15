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

import logging
from numbers import Number
from typing import Any
from asyncua import ua
from asyncua.common.node import Node
from datetime import datetime, timezone
from openfactory.assets import AssetAttribute
from openfactory.assets.utils import openfactory_timestamp
from .producer import GlobalAssetProducer
from .utils import opcua_data_timestamp, opcua_event_timestamp
from .gateway_metrics import MSG_SENT, SEND_LATENCY, LATEST_LATENCY


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
    def __init__(self, opcua_device_uuid: str, logger: logging.Logger, gateway_id: str, global_producer: GlobalAssetProducer):
        """
        Initialize the SubscriptionHandler.

        Args:
            opcua_device_uuid (str): UUID of the device.
            logger (logging.Logger): Logger instance for debug and error messages.
            gateway_id (str): Gateway ID
            global_producer (GlobalAssetProducer): Kafka producer of the Gateway

        Attributes:
            node_map (Dict[Node, Dict[str, str]]): Cache mapping OPC UA Node objects to metadata.
        """
        self.opcua_device_uuid = opcua_device_uuid
        self.logger = logger
        self.gateway_id = gateway_id
        self.global_producer = global_producer

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

        device_timestamp = opcua_data_timestamp(data.monitored_item.Value)
        self.global_producer.send(
            asset_uuid=self.opcua_device_uuid,
            asset_attribute=AssetAttribute(
                id=local_name,
                value=val,
                type=ofa_type,
                tag=browse_name,
                timestamp=openfactory_timestamp(device_timestamp),
            )
        )

        # Measure latency (seconds)
        latency = (datetime.now(timezone.utc) - device_timestamp).total_seconds()
        MSG_SENT.labels(gateway=self.gateway_id).inc()
        if latency >= 0:  # ignore clock skew issues
            LATEST_LATENCY.labels(gateway=self.gateway_id).set(latency)
            SEND_LATENCY.labels(gateway=self.gateway_id).observe(latency)

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

        device_timestamp = opcua_event_timestamp(event)
        self.global_producer.send(
            asset_uuid=self.opcua_device_uuid,
            asset_attribute=AssetAttribute(
                id="alarm",
                value=message_text,
                type="Condition",
                tag=tag,
                timestamp=openfactory_timestamp(device_timestamp),
            )
        )

        # Measure latency (seconds)
        latency = (datetime.now(timezone.utc) - device_timestamp).total_seconds()
        MSG_SENT.labels(gateway=self.gateway_id).inc()
        if latency >= 0:  # ignore clock skew issues
            LATEST_LATENCY.labels(gateway=self.gateway_id).set(latency)
            SEND_LATENCY.labels(gateway=self.gateway_id).observe(latency)
