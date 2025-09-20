# OPC UA Connectors

This module provides OPC UA connectivity for OpenFactory.

It contains two services:

## 🛰️ OPCUA-Coordinator

- Decides which `OPCUA-Gateway` should manage which devices.
- Keeps track of registered OPC UA devices and their assignments.
- Communicates assignment updates to gateways.

## 🌐 OPCUA-Gateway

- Connects to one or more OPC UA servers.
- Collects data from devices (variables, events, alarms).
- Streams normalized messages into Kafka.

Multiple gateways can be deployed for scalability.

## ⚙️ Configuration

### Environment Variables (Coordinator)
No environment variables are required

### Environment Variables (Gateway)

| Variable                  | Description                               | Default     |
|---------------------------|-------------------------------------------|-------------|
| `KAFKA_BROKER`            | Kafka bootstrap server address            | Must be set |
| `KSQLDB_URL`              | ksqlDB URL                                | Must be set |
| `OPCUA_GATEWAY_LOG_LEVEL` | Log level                                 | INFO        |

## 🚀 Deployment

### Local Docker
After setting the required environment variables, deploy the services with:

```bash
docker compose up -d
```

### Dcker Swarm
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
