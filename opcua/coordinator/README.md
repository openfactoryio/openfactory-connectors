# OPCUA Coordinator

The **OPCUA Coordinator** is a service that manages OPC UA Gateways and device assignments for the OpenFactory platform. It ensures that devices are dynamically assigned to gateways, allowing horizontal scalability and centralized management.

This README covers usage for both **developers** and **end users**.

## Table of Contents

* [Features](#features)
* [How it Works](#how-it-works)
* [API Endpoints](#api-endpoints)
* [Local Development](#local-development)
* [Environment Variables](#environment-variables)
* [Deployment](#deployment)

## Features

* Dynamic registration of OPC UA gateways.
* Device registration and round-robin assignment to gateways.
* Automatic notification of gateways for device add/remove operations.
* Retrieval of all device-to-gateway assignments.
* Simple REST API with FastAPI.

## How it Works

1. **Gateways** register themselves with the coordinator using `/register_gateway` during their deployment.
2. **Devices** are registered using `/register_device`, and are assigned to gateways automatically in a round-robin fashion.
3. The coordinator maintains an in-memory mapping of `device_uuid -> gateway`.
4. Gateways are notified when devices are added or removed.

For local development, a default gateway (`http://localhost:8000`) can be automatically added by setting the environment variable `COORDINATOR_LOCAL_DEV=1`.

## API Endpoints

### 1. Register a Gateway

**POST** `/register_gateway`
**Request Body**:

```json
{
  "gateway_host": "http://gateway-host:8000"
}
```

**Response**:

```json
{
  "status": "registered",
  "gateway_host": "http://gateway-host:8000"
}
```

### 2. Register a Device

**POST** `/register_device`
**Request Body**:

```json
{
  "device": { "<Device object as defined in openfactory.schemas.devices>" }
}
```

**Response**:

```json
{
  "device_uuid": "device-123",
  "assigned_gateway": "http://gateway-host:8000"
}
```

**Errors**:

* 400 if device is already registered.
* 500 if no gateways are registered.

### 3. Unregister a Device

**DELETE** `/unregister_device/{device_uuid}`
**Response**:

```json
{
  "status": "unregistered",
  "device_uuid": "device-123",
  "gateway": "http://gateway-host:8000"
}
```

### 4. List Assignments

**GET** `/assignments`
**Response**:

```json
{
  "device-123": "http://gateway-1:8000",
  "device-456": "http://gateway-2:8000"
}
```

### 5. List Gateways

**GET** `/gateways`
**Response**:

```json
[
  "http://gateway-1:8000",
  "http://gateway-2:8000"
]
```

---

## Local Development

1. Clone the repository.

2. Enable local development mode (adds default gateway `http://localhost:8000`):

```bash
export COORDINATOR_LOCAL_DEV=1
```

3. Run the coordinator:

```bash
python -m opcua.coordinator.src.coordinator
```

5. The API will be available at `http://localhost:8000`.

## Environment Variables

| Variable                | Description                                                                           | Required / Optional |
| ----------------------- | ------------------------------------------------------------------------------------- | ------------------- |
| `KAFKA_BROKER`          | Kafka bootstrap server address                                                        | Required            |
| `KSQLDB_URL`            | ksqlDB URL                                                                            | Required            |
| `COORDINATOR_LOCAL_DEV` | If set to `1`, adds a default local gateway (`http://localhost:8000`) for development | Optional            |

## Deployment

### Docker Compose

```bash
docker compose up -d
```
