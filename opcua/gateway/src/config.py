"""
Configuration constants for the OPC UA Gateway.

This module reads environment variables to configure the gateway
and provides default values when necessary.

Constants:
    KAFKA_BROKER (str): Kafka bootstrap servers, from `KAFKA_BROKER` env var.
    KSQLDB_URL (str): URL for the KSQLDB instance, from `KSQLDB_URL` env var.
    COORDINATOR_URL (str): URL of the OPC UA Coordinator, from `COORDINATOR_URL` env var.
    OPCUA_GATEWAY_PORT (int): Port for FastAPI/Uvicorn server, default 8000.
    LOG_LEVEL (str): Logging level, default "DEBUG", read from `OPCUA_GATEWAY_LOG_LEVEL`.
    KAFKA_SEND_INTERVAL_MS (float): Interval between sending queued messages to Kafka, in milliseconds, default 5.
    KAFKA_QUEUE_MAXSIZE (int): Maximum number of messages to hold in the internal async queue, default 1000.
"""

import os

# Kafka cluster
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KSQLDB_URL = os.getenv("KSQLDB_URL")

# Kafka producer
KAFKA_LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "5"))

# Connector configuration
COORDINATOR_URL = os.getenv("COORDINATOR_URL")
OPCUA_GATEWAY_PORT = int(os.getenv("OPCUA_GATEWAY_PORT", 8000))
LOG_LEVEL = os.getenv("OPCUA_GATEWAY_LOG_LEVEL", "DEBUG").upper()

# Internal async queue for messages
KAFKA_SEND_INTERVAL_MS = float(os.getenv("KAFKA_SEND_INTERVAL_MS", "5"))
KAFKA_QUEUE_MAXSIZE = int(os.getenv("KAFKA_QUEUE_MAXSIZE", "1000"))
