"""
Module for tracking the global state of OPC UA Gateway devices.

This module maintains in-memory registries for all active monitoring tasks
and registered device definitions.

Variables:
    _active_tasks (Dict[str, asyncio.Task]): Mapping of device UUIDs to their
        corresponding asyncio monitoring tasks.
    _active_device_defs (Dict[str, Device]): Mapping of device UUIDs to
        their registered Device schema instances.
"""

import asyncio
from typing import Dict
from openfactory.schemas.devices import Device

_active_tasks: Dict[str, asyncio.Task] = {}
_active_device_defs: Dict[str, Device] = {}
