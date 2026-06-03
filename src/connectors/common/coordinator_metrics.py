from prometheus_client import Info, Counter, Histogram
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

# Metrics definitions
BUILD_INFO = Info(
    "coordinator_build",
    "Build information for the OPCUA Coordinator"
)

DEVICE_ASSIGNMENTS_TOTAL = Counter(
    'device_assignments_total',
    'Total number of device assignments to OPCUA Gateways'
)

DEVICE_ASSIGNMENT_LATENCY = Histogram(
    'device_assignment_latency_seconds',
    'Time spent assigning a device to a OPCUA Gateway'
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
