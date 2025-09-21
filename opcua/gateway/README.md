# OPCUA Gateway

The **OPCUA Gateway** is a FastAPI-based service that connects to OPC UA devices, monitors them, and streams their data to OpenFactory (e.g., via Kafka).

It is responsible for:

* Registering devices dynamically from the Coordinator.
* Starting/stopping monitoring tasks for each device.
* Exposing device status via a REST API.

## Table of Contents

* [Features](#features)
* [How it Works](#how-it-works)
* [API Endpoints](#api-endpoints)
* [Environment Variables](#environment-variables)
* [Local Development](#local-development)
* [Deployment](#deployment)
* [Project Structure](#-project-structure)

## Features

* Register and monitor multiple OPC UA devices.
* Start and stop monitoring tasks dynamically.
* Report device status, including task health.
* Integration-ready with OpenFactory (Kafka and ksqlDB).


## How it Works

1. The gateway exposes REST endpoints to **add**, **remove**, and **list devices**.
2. When a device is added, a background task (`monitor_device`) is created to collect data.
3. When a device is removed, its task is canceled, and the device is removed from the registry.
4. The gateway is dynamically controlled by the OPC UA Coordinator.

## API Endpoints

### 1. Add a Device

**POST** `/add_device`
**Request Body**:

```json
{
  "device": { "<Device object as defined in openfactory.schemas.devices>" }
}
```

**Response**:

```json
{
  "status": "started",
  "device_uuid": "device-123"
}
```

**Errors**:

* 400 if the device is already registered.

### 2. Remove a Device

**DELETE** `/remove_device/{device_uuid}`

**Response**:

```json
{
  "status": "removed",
  "device_uuid": "device-123"
}
```

**Errors**:

* 404 if the device is not found.

### 3. List Devices

**GET** `/devices`

**Response**:

```json
{
  "device-123": {
    "uuid": "device-123",
    "connector": "opcua-connector-1",
    "task_running": true
  }
}
```

* `task_running` indicates whether the monitoring task is active.
* `connector` shows optional connector metadata from the device object.


## Environment Variables

| Variable                  | Description                       | Required / Optional       |
| ------------------------- | --------------------------------- | ------------------------- |
| `KAFKA_BROKER`            | Kafka bootstrap server address    | Required                  |
| `KSQLDB_URL`              | ksqlDB server URL                 | Required                  |
| `OPCUA_GATEWAY_PORT`      | Port for FastAPI/Uvicorn server   | Optional (default: 8001)  |
| `OPCUA_GATEWAY_LOG_LEVEL` | Logging level (DEBUG, INFO, etc.) | Optional (default: DEBUG) |

## Local Development

1. Clone the repository and navigate to the gateway folder.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set required environment variables:

```bash
export KAFKA_BROKER=your_kafka_address
export KSQLDB_URL=your_ksqldb_url
```

4. Start the gateway:

```bash
python -m opcua.gateway.src.gateway
```

5. The API will be available at `http://localhost:8001` (or your configured port).


## Deployment

### Docker

```bash
docker build -t opcua-gateway ./gateway
docker run -p 8001:8001 \
  -e KAFKA_BROKER=your_kafka_address \
  -e KSQLDB_URL=your_ksqldb_url \
  opcua-gateway
```

### Docker Compose

```bash
docker compose up -d
```

## 📂 Project Structure

```
gateway/
├── __init__.py
├── main.py          # Entry point (FastAPI + Uvicorn)
├── api.py           # REST API endpoints (/add_device, /remove_device, /devices)
├── monitor.py       # DeviceMonitor class and monitoring loop
├── subscription.py  # SubscriptionHandler for data changes & events
├── producer.py      # Global Kafka producer wrapper
├── utils.py         # Logging setup + timestamp helpers
├── state.py         # Global task/device registries
└── config.py        # Config (env vars, ports, log levels, etc.)
```
