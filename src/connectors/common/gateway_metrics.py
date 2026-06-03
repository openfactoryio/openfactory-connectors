from prometheus_client import Info, Gauge
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Metrics definitions
BUILD_INFO = Info(
    "gateway_build",
    "Build information for the Gateway"
)

GATEWAY_DEVICE_COUNT = Gauge(
    'gateway_devices_count',
    'Number of devices currently assigned to a Gateway'
)

KAFKA_TX_MESSAGES = Gauge(
    "kafka_tx_messages_total",
    "Messages transmitted by librdkafka"
)

KAFKA_TX_BYTES = Gauge(
    "kafka_tx_bytes_total",
    "Bytes transmitted by librdkafka"
)

KAFKA_BATCH_COUNT_AVG = Gauge(
    "kafka_batch_count_avg",
    "Average number of messages per Kafka Producer request"
)

KAFKA_BATCH_COUNT_P95 = Gauge(
    "kafka_batch_count_p95",
    "95th percentile number of messages per Kafka Producer request"
)

KAFKA_BROKER_RTT_AVG = Gauge(
    "kafka_broker_rtt_avg_seconds",
    "Average Kafka broker RTT in seconds",
    ["broker"]
)

KAFKA_BROKER_RTT_P95 = Gauge(
    "kafka_broker_rtt_p95_seconds",
    "95th percentile Kafka broker RTT in seconds",
    ["broker"]
)

KAFKA_QUEUE_MESSAGES = Gauge(
    "kafka_queue_messages",
    "Messages queued inside librdkafka"
)

KAFKA_QUEUE_BYTES = Gauge(
    "kafka_queue_bytes",
    "Bytes queued inside librdkafka"
)

KAFKA_BROKER_TX_ERRORS = Gauge(
    "kafka_tx_errors_total",
    "Kafka transmission errors",
    ["broker"]
)


def metrics_endpoint():
    """
    FastAPI endpoint for Prometheus scraping.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
