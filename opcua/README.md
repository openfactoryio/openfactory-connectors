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

### Environment Variables (Gateway)

| Variable         | Description                               | Default |
|------------------|-------------------------------------------|---------|
| `KAFKA_BROKER`   | Kafka bootstrap server address            | `localhost:9092` |
| `KAFKA_TOPIC`    | Kafka topic to stream data into           | `opcua_data` |
| `DEVICES`        | Comma-separated list of OPC UA endpoints  | - |

*(Coordinator config TBD)*

---

## 🚀 Deployment

Typical deployment flow:

```

OPCUA-Coordinator -> \[assigns devices] -> OPCUA-Gateway -> Kafka

```

You can run the services in Docker Swarm or Kubernetes.  
Each Gateway manages a subset of devices for load balancing.

---

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
