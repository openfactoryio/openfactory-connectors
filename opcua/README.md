# OPC UA Connector for OpenFactory

The **OPC UA Connector** enables seamless integration between industrial equipment that speaks the **OPC UA protocol** and the **OpenFactory platform**.

It acts as a bridge between **shop-floor devices** and the **cloud-native data infrastructure**:

* **Devices (OPC UA servers)** expose machine data (measurements, alarms, events).
* **OPCUA-Gateways** connect to those devices, collect the raw data, normalize it, and publish it into **Kafka**, OpenFactory’s event backbone.
* **OPCUA-Coordinator** orchestrates multiple gateways, deciding which gateway should handle which devices. This allows OpenFactory to scale horizontally and balance the load across gateways.

In short:
- The **Coordinator** ensures **smart distribution of devices across gateways**.
- The **Gateways** ensure **reliable data ingestion from machines into Kafka**.

Together, they allow OpenFactory to handle **many devices across multiple factories** while staying resilient, scalable, and simple to manage.

## 🛰️ OPCUA-Coordinator

- Receives requests to connect OPC UA devices to OpenFactory.
- Decides which `OPCUA-Gateway` should manage which devices.
- Keeps track of registered OPC UA devices and their assignments.
- Communicates assignment updates to gateways.
- Detailed usage and API documentation: [📖 Coordinator README](coordinator/README.md)

## 🌐 OPCUA-Gateway

- Connects to one or more OPC UA servers.
- Collects data from devices (variables, events, alarms).
- Streams normalized messages into OpenFactory (Kafka).
- Multiple gateways can be deployed for scalability.
- Detailed usage and API documentation: [📖 Gateway README](gateway/README.md)

## ⚙️ Configuration

### Environment Variables (Coordinator)

| Variable       | Description                    | Default     |
| -------------- | ------------------------------ | ----------- |
| `KAFKA_BROKER` | Kafka bootstrap server address | Must be set |
| `KSQLDB_URL`   | ksqlDB URL                     | Must be set |

### Environment Variables (Gateway)

| Variable                  | Description                    | Default     |
| ------------------------- | ------------------------------ | ----------- |
| `KAFKA_BROKER`            | Kafka bootstrap server address | Must be set |
| `KSQLDB_URL`              | ksqlDB URL                     | Must be set |
| `OPCUA_GATEWAY_LOG_LEVEL` | Log level                      | INFO        |

## 📋 Prerequisites

Before deploying the OPC UA Connector stack, make sure you have an OpenFactory infrastructure up and running. This includes:

* A running **Kafka** cluster and **ksqlDB** instance.
* A **Docker network** created for your factory services

For more details refere to the documentation in [openfactory-core](https://github.com/openfactoryio/openfactory-core).


## 🚀 Deployment on an OpenFactory Cluster

### 1️⃣ Configure OPC UA Connector stack

Create a `docker-compose.yml` with your desired configuration.
Example for **two OPC UA Gateways**:

```yml
services:
  opcua-gateway-1:
    image: ghcr.io/openfactoryio/opcua-gateway:<VERSION>
    environment:
      - GATEWAY_HOST=opcua-gateway-1
      - COORDINATOR_URL=http://opcua-coordinator:8000
      - KAFKA_BROKER=${KAFKA_BROKER}
      - KSQLDB_URL=${KSQLDB_URL}
      - OPCUA_GATEWAY_LOG_LEVEL=INFO
    networks:
      - factory-net

  opcua-gateway-2:
    image: ghcr.io/openfactoryio/opcua-gateway:<VERSION>
    environment:
      - GATEWAY_HOST=opcua-gateway-2
      - COORDINATOR_URL=http://opcua-coordinator:8000
      - KAFKA_BROKER=${KAFKA_BROKER}
      - KSQLDB_URL=${KSQLDB_URL}
      - OPCUA_GATEWAY_LOG_LEVEL=INFO
    networks:
      - factory-net

  opcua-coordinator:
    image: ghcr.io/openfactoryio/opcua-coordinator:<VERSION>
    environment:
      - KAFKA_BROKER=${KAFKA_BROKER}
      - KSQLDB_URL=${KSQLDB_URL}
    ports:
      - "8000:8000"
    networks:
      - factory-net

networks:
  factory-net:
    external: true
```

* Replace `<VERSION>` with the matching **OpenFactory platform version** (e.g. `v1.2.3`).
* You can set environment variables directly in the file instead of importing local environment variables with `${...}`.

### 2️⃣ Deploy the stack

```bash
docker stack deploy -c docker-compose.yml opcua
```

This will launch the Coordinator and Gateways inside your OpenFactory cluster.

## 🧪 Deployment for Developers

For local development, you can use the provided `docker-compose.yml` in this repo.

### Local Docker

```bash
docker compose up -d
```

### Docker Swarm

```bash
docker stack deploy -c docker-compose.yml opcua
```

## 📂 Directory Structure

```
opcua/
├── coordinator/
│   ├── Dockerfile
│   └── src/...
└── gateway/
    ├── Dockerfile
    └── src/...
```
