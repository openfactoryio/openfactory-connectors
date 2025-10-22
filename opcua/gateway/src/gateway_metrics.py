from prometheus_client import Histogram, Gauge, Counter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Metrics definitions
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

MSG_QUEUE = Gauge(
    "opcua_messages_queue",
    "Current number of messages in the internal async queue awaiting Kafka send",
    ["gateway"]
)

RECIEVE_LATENCY = Gauge(
    "opcua_delivery_latency_seconds",
    "Time difference between the OPC UA device timestamp and when the gateway receives the value (seconds)",
    ["gateway"]
)

SEND_LATENCY = Histogram(
    "opcua_kafka_send_latency_seconds",
    "Time difference between OPC UA data timestamp and Kafka send time (seconds)",
    ["gateway"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1)
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
