from prometheus_client import Info, Histogram, Gauge, Counter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Metrics definitions
BUILD_INFO = Info(
    "opcua_gateway_build",
    "Build information for the OPCUA Gateway"
)

EVENT_LOOP_LAG = Histogram(
    "opcua_event_loop_lag_seconds",
    "AsyncIO event loop lag",
    ["gateway"],
    buckets=(0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, float("inf")),
)

EVENT_LOOP_SCHEDULING_DELAY = Histogram(
    "opcua_event_loop_scheduling_delay_seconds",
    "Time between enqueue and actual execution of a task in the event loop",
    buckets=(0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, float("inf")),
    labelnames=("gateway",),
)

MSG_SENT = Counter(
    "opcua_messages_sent_total",
    "Total messages sent to Kafka",
    ["gateway"]
)

QUEUE_SIZE = Gauge(
    "opcua_queue_size",
    "Number of messages in internal queue",
    ["gateway"]
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
