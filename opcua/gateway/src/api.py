"""
API endpoints for the OPC UA Gateway.

This module defines FastAPI routes to manage OPC UA devices:

- POST /add_device: Register a new device and start its monitoring task.
- DELETE /remove_device/{device_uuid}: Stop monitoring and remove a device.
- GET /devices: List all registered devices with their status.

All endpoints interact with the global device/task registries
(`_active_device_defs` and `_active_tasks`) and use the FastAPI
`app.state.logger` for logging.
"""

import asyncio
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from openfactory.schemas.devices import Device
from .state import _active_tasks, _active_device_defs
from .monitor import monitor_device


class AddDeviceRequest(BaseModel):
    """
    Request model for adding a new OPC UA device.

    Attributes:
        device (Device): Device schema instance to register and monitor.
    """
    device: Device


router = APIRouter()


@router.post("/add_device")
async def add_device(req: AddDeviceRequest, request: Request) -> Dict[str, str]:
    """
    Register a new device and start its monitoring task.

    Args:
        req (AddDeviceRequest): Request object containing the Device schema.
        request (Request): FastAPI request object, used to access the app logger.

    Raises:
        HTTPException: If the device UUID is already registered.

    Returns:
        Dict[str, str]: Status and UUID of the registered device.
    """
    device = req.device
    if device.uuid in _active_tasks:
        raise HTTPException(status_code=400, detail=f"Device {device.uuid} already added")
    task = asyncio.create_task(monitor_device(device, request.app))
    _active_tasks[device.uuid] = task
    return {"status": "started", "device_uuid": device.uuid}


@router.delete("/remove_device/{device_uuid}")
async def remove_device(device_uuid: str) -> Dict[str, str]:
    """
    Stop monitoring and remove a device by its UUID.

    Args:
        device_uuid (str): UUID of the device to remove.

    Raises:
        HTTPException: If the device is not found in the registry.

    Returns:
        Dict: Status and UUID of the removed device.
    """
    task = _active_tasks.pop(device_uuid, None)
    _active_device_defs.pop(device_uuid, None)
    if not task:
        raise HTTPException(status_code=404, detail="Device not found")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return {"status": "removed", "device_uuid": device_uuid}


@router.get("/devices_count")
async def count_devices() -> int:
    """
    Retrieve the number of currently registered devices.

    Returns:
        int: number of currently registered devices.
    """
    return len(_active_device_defs)


@router.get("/devices")
async def list_devices() -> Dict[str, Dict[str, Any]]:
    """
    List all registered devices with their connector info and task status.

    Returns:
        Dict: Mapping of device UUIDs to a dictionary containing 'uuid', 'connector', and 'task_running' status.
    """
    return {
        uuid: {
            "uuid": uuid,
            "connector": getattr(dev, "connector", None),
            "task_running": (not _active_tasks[uuid].done()) if uuid in _active_tasks else False,
        }
        for uuid, dev in _active_device_defs.items()
    }


@router.get("/status")
async def status(request: Request) -> Dict[str, str]:
    """
    Returrns Gateway status.

    Returns:
        Dict: A dictionary containing a single key `"status"`, whose value is either
        `"AVAILABLE"` if the gateway is active, or `"UNAVAILABLE"` otherwise.
    """
    if request.app.state.gateway_id == 'UNAVAILABLE':
        return {"status": "UNAVAILABLE"}
    else:
        return {"status": "AVAILABLE"}
