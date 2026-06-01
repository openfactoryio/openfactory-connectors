# OPC UA Connector for OpenFactory

The **OPC UA Connector** enables seamless integration between industrial equipment that speaks the **OPC UA protocol** and the **OpenFactory platform**.

It acts as a bridge between **shop-floor devices** and the **cloud-native data infrastructure**:

* **OPC UA Devices** expose machine data.
* **OPCUA-Gateways** connect to those devices, collect the raw data, normalize it, and publish it into **Kafka**, OpenFactory’s event backbone.
* **OPCUA-Coordinator** orchestrates multiple gateways, deciding which gateway should handle which devices. This allows OpenFactory to scale horizontally and balance the load across gateways.

In short:
- The **Coordinator** ensures **smart distribution of devices across gateways**.
- The **Gateways** ensure **reliable data ingestion from machines into Kafka**.

Together, they allow OpenFactory to handle **many devices across the factory** while staying resilient, scalable, and simple to manage.

## 🛰️ OPCUA-Coordinator

- Receives requests to connect OPC UA devices to OpenFactory.
- Decides which `OPCUA-Gateway` should manage which devices.
- Communicates assignment updates to gateways.

## 🌐 OPCUA-Gateway

- Connects to one or more OPC UA devices.
- Collects data from devices.
- Streams normalized messages into OpenFactory (Kafka).
- Multiple gateways can be deployed for scalability.

## ⚙️ Configuration

### Environment Variables (Coordinator)

| Variable                      | Description                    | Default     |
| ----------------------------- | ------------------------------ | ----------- |
| `OPCUA_COORDINATOR_LOG_LEVEL` | Log level                      | INFO        |

### Environment Variables (Gateway)

| Variable                  | Description                           | Default     |
| ------------------------- | ------------------------------------- | ----------- |
| `OPCUA_GATEWAY_LOG_LEVEL` | Log level                             | INFO        |
| `KAFKA_LINGER_MS`         | Kafka producer linger in ms           | 5           |

## 📋 Prerequisites

Before deploying the Connector stack, make sure you have an OpenFactory infrastructure up and running. This includes:

* A running **Kafka** cluster and **ksqlDB** instance.
* A **Docker network** created for your factory services

For more details refere to the documentation in [openfactory-core](https://github.com/openfactoryio/openfactory-core).


## 🚀 Deployment on an OpenFactory Cluster

### 1️⃣ Configure OPC UA Connector stack

Both, coordinator and gateway, are OpenFactory applications.

Create a `opcua-connector.yml` OpenFActory configuration file with your desired configuration.
Example for **two Gateways**:

```yml
apps:

  opcua-coordinator:
    uuid: OPCUA-COORDINATOR
    image: ghcr.io/openfactoryio/opcua-coordinator:<VERSION>
    environment:
      - LOG_LEVEL=DEBUG
    networks:
      - factory-net

  opcua-gateway-1:
    uuid: OPCUA-GATEWAY-1
    image: ghcr.io/openfactoryio/opcua-gateway:<VERSION>
    environment:
      - LOG_LEVEL=DEBUG
      - KAFKA_LINGER_MS=10
    networks:
      - factory-net

  opcua-gateway-2:
    uuid: OPCUA-GATEWAY-2
    image: ghcr.io/openfactoryio/opcua-gateway:<VERSION>
    environment:
      - LOG_LEVEL=DEBUG
      - KAFKA_LINGER_MS=10
    networks:
      - factory-net
```

Notes:
* Replace `<VERSION>` with the matching **OpenFactory platform version** (e.g. `v1.2.3`).
* You can set environment variables directly in the file instead of importing local environment variables with `${...}`.
* The `uuid` of the coordinator and the gateway can be freely choosen and are discovered by the applications. 

### 2️⃣ Deploy the stack

```bash
ofa apps up opcua-connector.yml
```

This will launch the Coordinator and Gateways inside your OpenFactory cluster.
