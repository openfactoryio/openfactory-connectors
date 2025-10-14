from prometheus_client import Histogram, Gauge, Counter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Metrics definitions
MSG_SENT = Counter(
    "opcua_messages_sent_total",
    "Total messages sent to Kafka",
    ["gateway"]
)

SEND_LATENCY = Histogram(
    "opcua_kafka_send_latency_seconds",
    "Time difference between OPC UA data timestamp and Kafka send time (seconds)",
    ["gateway"]
)

LATEST_LATENCY = Gauge(
    "opcua_kafka_send_latency_latest_seconds",
    "Latest measured latency from OPC UA data timestamp to successful Kafka send (seconds)",
    ["gateway"]
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
