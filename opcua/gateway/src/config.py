"""
Configuration constants for the OPC UA Gateway.

This module reads environment variables to configure the gateway
and provides default values when necessary.

Constants:
    KAFKA_BROKER (str): Kafka bootstrap servers, from `KAFKA_BROKER` env var.
    KSQLDB_URL (str): URL for the KSQLDB instance, from `KSQLDB_URL` env var.
    OPCUA_GATEWAY_PORT (int): Port for FastAPI/Uvicorn server, default 8001.
    LOG_LEVEL (str): Logging level, default "DEBUG", read from `OPCUA_GATEWAY_LOG_LEVEL`.
"""

import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KSQLDB_URL = os.getenv("KSQLDB_URL")
OPCUA_GATEWAY_PORT = int(os.getenv("OPCUA_GATEWAY_PORT", 8001))
LOG_LEVEL = os.getenv("OPCUA_GATEWAY_LOG_LEVEL", "DEBUG").upper()
