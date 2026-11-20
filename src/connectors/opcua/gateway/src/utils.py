
"""
Utility functions for the OPC UA Gateway.

This module provides helper functions for:

- Extracting timestamps from OPC UA DataValues and events.
- Finding OPC UA nodes using browse paths.
- Normalizing OPC UA values into Python-native representations.

Functions:
- opcua_data_timestamp(data)
- opcua_event_timestamp(event)
- get_node_by_path(client, path)
- normalize_value(val)
"""

from typing import Any
from asyncua import ua, Client, Node
from datetime import datetime, timezone


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
        datetime: The selected timestamp. If no timestamp is
            available, the current UTC time is used.
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
        datetime: The selected timestamp. If no timestamp is
            available, the current UTC time is used.
    """
    event_time = getattr(event, "Time", None)
    receive_time = getattr(event, "ReceiveTime", None)

    if hasattr(event_time, "isoformat"):
        return event_time
    if hasattr(receive_time, "isoformat"):
        return receive_time
    return datetime.now(timezone.utc)


async def get_node_by_path(client: Client, path: str) -> Node:
    """
    Find an OPC UA node using a slash-separated browse path.

    Each part of the path contains a namespace index and a node name.
    Example: "0:Objects/2:MyDevice/2:Sensor1"

    If the path starts with "Root", it is skipped because
    the search already begins at the OPC UA Root node.

    Args:
        client (Client): Connected OPC UA client.
        path (str): Slash-separated list of browse path segments in
            the form ``"namespace_index:BrowseName"``.

    Returns:
        Node: The OPC UA node found at the given path.

    Raises:
        ValueError: If the path cannot be resolved.
    """
    elements = path.split("/")

    # Skip Root if it is already included in the path.
    if elements[0].endswith("Root"):
        elements = elements[1:]

    root = client.get_root_node()

    try:
        node = await root.get_child(elements)
        return node

    except ua.UaStatusCodeError as exc:
        # Find which part of the path could not be resolved.
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

        # List available child nodes to help explain the error.
        try:
            children = await node.get_children()
            child_names = []
            for c in children:
                bname = await c.read_browse_name()
                child_names.append(f"{bname.NamespaceIndex}:{bname.Name}")
        except Exception:
            child_names = []

        # Build a useful error message showing where the search failed.
        available_children = ", ".join(child_names) if child_names else "<none>"
        msg = (
            f"Failed to resolve OPC UA path: '{path}'.\n"
            f"Could not resolve segment: '{failed_segment}'.\n"
            f"Path resolved up to: '{'/'.join(resolved_segments) or '<root>'}'.\n"
            f"Server error: {exc}.\n"
            f"Children available at this level: {available_children}"
        )

        raise ValueError(msg) from exc


def normalize_value(val: object) -> Any:
    """
    Convert OPC UA-specific value types into JSON-serializable
    Python-native representations.

    Handles text, bytes, lists, variants, and timestamps.
    Values that are not recognized are returned unchanged.

    Args:
        val (object): Value received from OPC UA.

    Returns:
        Any: Normalized Python representation of the input value, or the original value.
    """
    # Return the text stored inside an OPC UA LocalizedText.
    if isinstance(val, ua.LocalizedText):
        return val.Text

    # Convert bytes into a list of integers for JSON.
    if isinstance(val, (bytes, bytearray)):
        return list(val)

    # Normalize each element of a list.
    if isinstance(val, list):
        return [normalize_value(v) for v in val]

    # Extract the value stored inside an OPC UA Variant.
    if isinstance(val, ua.Variant):
        return normalize_value(val.Value)

    # Convert timestamps into strings.
    if isinstance(val, datetime):
        return str(val)

    # Fallback: return as-is
    return val
