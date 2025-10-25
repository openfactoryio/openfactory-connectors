from prometheus_client import Info, Histogram, Gauge, Counter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Metrics definitions
BUILD_INFO = Info(
    "opcua_gateway_build",
    "Build information for the OPCUA gateway"
)

EVENT_LOOP_LAG = Gauge(
    "event_loop_lag_seconds",
    "AsyncIO event loop lag",
    ["gateway"]
)

MSG_SENT = Counter(
    "opcua_messages_sent_total",
    "Total messages sent to Kafka",
    ["gateway"]
)

BATCH_SIZE = Gauge(
    "opcua_batch_size",
    "Number of messages in none empty batch",
    ["gateway"]
)

BATCH_PROCESSED = Counter(
    "opcua_batches_processed_total",
    "Total batches processed (including empty ones)",
    ["gateway"]
)

KAFKA_BATCH_PROCESSING_DURATION = Histogram(
    "opcua_batch_processing_duration_seconds",
    "Time spent draining the queue and sending a none-empty batch of messages to Kafka",
    ["gateway"],
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, float("inf")),
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
