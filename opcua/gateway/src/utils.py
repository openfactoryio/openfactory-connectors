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
from openfactory.assets.utils import openfactory_timestamp, current_timestamp
from asyncua import ua


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
    if hasattr(event_time, "isoformat"):
        return openfactory_timestamp(event_time)
    if hasattr(receive_time, "isoformat"):
        return openfactory_timestamp(receive_time)
    return current_timestamp()
