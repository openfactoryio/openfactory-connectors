from prometheus_client import Histogram
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Metrics definitions
KAFKA_SEND_LATENCY = Histogram(
    "opcua_kafka_send_latency_seconds",
    "Time difference between OPC UA data timestamp and Kafka send time (seconds)",
    ["gateway"]
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
