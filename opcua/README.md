# OPC UA Connector for OpenFactory

The **OPC UA Connector** enable seamless integration between industrial equipment that speaks the **OPC UA protocol** and the **OpenFactory data platform**.

They act as the bridge between shop-floor devices and the cloud-native data infrastructure:

* **Devices (OPC UA servers)** expose machine data (measurements, alarms, events).
* **OPCUA-Gateways** connect to those devices, collect the raw data, normalize it, and publish it into **Kafka**, OpenFactory’s event backbone.
* **OPCUA-Coordinator** orchestrates multiple gateways, deciding which gateway should handle which devices. This allows OpenFactory to scale horizontally and balance the load across gateways.

In short:
- 👉 The Coordinator ensures **smart distribution of devices across gateways**.
- 👉 The Gateways ensure **reliable data ingestion from machines into Kafka**.

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

No environment variables are required.

### Environment Variables (Gateway)

| Variable                  | Description                    | Default     |
| ------------------------- | ------------------------------ | ----------- |
| `KAFKA_BROKER`            | Kafka bootstrap server address | Must be set |
| `KSQLDB_URL`              | ksqlDB URL                     | Must be set |
| `OPCUA_GATEWAY_LOG_LEVEL` | Log level                      | INFO        |

## 🚀 Deployment

### Local Docker
After setting the required environment variables, deploy the services with:

```bash
docker compose up -d
```

### Docker Swarm

After setting the required environment variables, deploy the services with:
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
