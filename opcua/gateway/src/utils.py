"""
Utility functions for the OPC UA Gateway.

This module provides:
- Logging setup (`setup_logging`) for consistent logging across the gateway.
- Timestamp extraction functions for OPC UA DataValues and events,
  converting them into OpenFactory-compatible timestamps.

Functions:
- `setup_logging(name, level)`: Creates and configures a logger.
- `opcua_data_timestamp(data)`: Extracts the most relevant timestamp from a DataValue.
- `opcua_event_timestamp(event)`: Extracts the most relevant timestamp from an OPC UA event.
"""

import logging
from typing import Any
from asyncua import ua, Client, Node
from datetime import datetime, timezone


def setup_logging(name: str = "opcua.gateway", level: str = "DEBUG") -> logging.Logger:
    """
    Create and configure a logger.

    If the logger already has handlers, it will not add additional handlers.

    Args:
        name (str, optional): Logger name. Defaults to "opcua.gateway".
        level (str, optional): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to "DEBUG".

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
        logger.addHandler(ch)
    return logger


def opcua_data_timestamp(data: ua.DataValue) -> datetime:
    """
    Extract the most relevant timestamp from an OPC UA DataValue.

    Priority order:
        1. SourceTimestamp (preferred, device time)
        2. ServerTimestamp (fallback, server processing time)
        3. Current system time (last resort)

    Args:
        data (ua.DataValue): The OPC UA DataValue to extract timestamps from.

    Returns:
        datetime: The extracted timestamp as a UTC-aware datetime object.
    """
    if data.SourceTimestamp:
        return data.SourceTimestamp
    if data.ServerTimestamp:
        return data.ServerTimestamp
    return datetime.now(timezone.utc)


def opcua_event_timestamp(event: Any) -> datetime:
    """
    Extract the most relevant timestamp from an OPC UA event.

    Priority order:
        1. event.Time
        2. event.ReceiveTime
        3. Current UTC time

    Args:
        event (Any): OPC UA event object.

    Returns:
        datetime: The extracted timestamp as a UTC-aware datetime object.
    """
    event_time = getattr(event, "Time", None)
    receive_time = getattr(event, "ReceiveTime", None)
    if hasattr(event_time, "isoformat"):
        return event_time
    if hasattr(receive_time, "isoformat"):
        return receive_time
    return datetime.now(timezone.utc)


async def get_node_by_path(client: Client, path: str) -> Node | None:
    """
    Resolve and return an OPC UA node using a comma-separated browse path.

    The path must be given in the format ``"ns:Name,ns:ChildName,..."``,
    where each segment specifies a namespace index and a browse name.
    For example: ``"0:Objects,2:MyDevice,2:Sensor1"``.

    If the first element ends with ``"Root"``, it will be skipped because
    the search always starts from the OPC UA Root node.

    Args:
        client (Client): Connected OPC UA client
        path (str): Comma-separated list of browse path segments in the form ``"namespace_index:BrowseName"``

    Returns:
        Node: The resolved OPC UA node corresponding to the provided path.

    Raises:
        ValueError:
            If any segment in the provided path cannot be resolved.
    """
    elements = path.split("/")

    # Normalize: skip Root if included
    if elements[0].endswith("Root"):
        elements = elements[1:]

    root = client.get_root_node()

    try:
        node = await root.get_child(elements)
        return node

    except ua.UaStatusCodeError as exc:
        # Find out where it broke
        node = root
        resolved_segments = []

        failed_segment = None

        for segment in elements:
            try:
                node = await node.get_child([segment])
                resolved_segments.append(segment)
            except ua.UaStatusCodeError:
                failed_segment = segment
                break

        # browse children at the level where the failure occurred
        try:
            children = await node.get_children()
            child_names = []
            for c in children:
                bname = await c.read_browse_name()
                child_names.append(f"{bname.NamespaceIndex}:{bname.Name}")
        except Exception:
            child_names = []

        # Build error message
        available_children = ", ".join(child_names) if child_names else "<none>"
        msg = (
            f"Failed to resolve OPC UA path: '{path}'.\n"
            f"Could not resolve segment: '{failed_segment}'.\n"
            f"Path resolved up to: '{'/'.join(resolved_segments) or '<root>'}'.\n"
            f"Server error: {exc}.\n"
            f"Children available at this level: {available_children}"
        )

        raise ValueError(msg) from exc
