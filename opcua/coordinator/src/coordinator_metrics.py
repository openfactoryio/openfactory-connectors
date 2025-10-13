import logging
from prometheus_client import Gauge, Counter, Histogram
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response, HTTPException

logger = logging.getLogger(__name__)

# Metrics definitions
GATEWAY_DEVICE_COUNT = Gauge(
    'opcua_gateway_devices_count',
    'Number of devices currently assigned to a OPCUA Gateway',
    ['gateway_id']
)

DEVICE_ASSIGNMENTS_TOTAL = Counter(
    'opcua_device_assignments_total',
    'Total number of devices assigned to OPCUA Gateways'
)

DEVICE_ASSIGNMENT_LATENCY = Histogram(
    'opcua_device_assignment_latency_seconds',
    'Time spent assigning a device to a OPCUA Gateway'
)


def update_gateway_metrics(gateway_results: dict):
    """
    Update Prometheus metrics for all gateways.

    Args:
        gateway_results (dict): {gateway_id: {"host": str, "devices_count": int}}
    """
    # First, check if all gateways are unreachable
    if all(info["devices_count"] == float('inf') for info in gateway_results.values()):
        raise HTTPException(status_code=503, detail="All gateways unreachable")

    # Update Prometheus gauge for each gateway
    for gw_id, info in gateway_results.items():
        count = info["devices_count"]
        # Use 0 if unreachable (float('inf'))
        GATEWAY_DEVICE_COUNT.labels(gateway_id=gw_id).set(0 if count == float('inf') else count)


def increment_assignment_counter():
    DEVICE_ASSIGNMENTS_TOTAL.inc()


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
