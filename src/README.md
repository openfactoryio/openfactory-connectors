# OpenFactory Connector Coordination Framework

The **OpenFactory Connector Coordination Framework** provides a scalable architecture for distributed device connectors.

It consists of two components:

* A **Coordinator**, responsible for assigning devices to gateways.
* One or more **Gateways**, responsible for connecting to devices and managing their lifecycle.

The framework is connector-agnostic and can be used to implement OPCUA, Modbus, MQTT, BACnet, or any other device integration.

Device assignments are persisted through Kafka and ksqlDB, allowing gateways and coordinators to recover automatically after restarts or failures.

## Table of Contents

* [Features](#features)
* [Architecture](#architecture)
* [How it Works](#how-it-works)
* [Coordinator Responsibilities](#coordinator-responsibilities)
* [Gateway Responsibilities](#gateway-responsibilities)
* [Assignment Persistence](#assignment-persistence)
* [Startup Sequence](#startup-sequence)
* [OpenFactory Methods](#openfactory-methods)
* [Creating a New Connector](#creating-a-new-connector)
* [Recovery and Fault Tolerance](#recovery-and-fault-tolerance)
* [Metrics](#metrics)
* [Environment Variables](#environment-variables)
* [Deployment](#deployment)

---

## Features

* Automatic discovery of coordinators and gateways.
* Dynamic gateway registration.
* Dynamic device assignment.
* Persistent assignment storage through Kafka and ksqlDB.
* Automatic gateway state reconstruction after restart.
* OpenFactory Asset-based communication.
* Horizontal scaling through multiple gateways.
* Prometheus metrics integration.
* Connector-independent architecture.

---

## Architecture

```text
                +------------------+
                |   Coordinator    |
                +------------------+
                         |
         +---------------+---------------+
         |                               |
         |                               |
+------------------+         +------------------+
|    Gateway A     |         |    Gateway B     |
+------------------+         +------------------+
         |                               |
     Devices                         Devices
```

The Coordinator manages assignment decisions while Gateways maintain active connections to devices.

Communication occurs through OpenFactory Assets, Kafka, and ksqlDB.

---

## How it Works

### Gateway Registration

When a Gateway starts:

1. It discovers the Coordinator Asset.
2. It waits for the Coordinator to become available.
3. It registers itself with the Coordinator.

The Coordinator maintains a list of available gateways and can begin assigning devices to them.

### Device Registration

When a device is registered:

1. The Coordinator selects a gateway using a connector-specific assignment strategy.
2. The selected gateway receives a `register_device()` request.
3. The device assignment is recorded in Kafka and ksqlDB.
4. The gateway connects to the device.

### Device Deregistration

When a device is removed:

1. The Coordinator identifies the assigned gateway.
2. The gateway receives a `deregister_device()` request.
3. The assignment record is removed.

---

## Coordinator Responsibilities

The Coordinator is responsible for:

* Discovering deployed gateways.
* Maintaining the gateway registry.
* Assigning devices to gateways.
* Tracking device-to-gateway assignments.
* Persisting assignments in Kafka and ksqlDB.
* Routing registration and deregistration requests.

The Coordinator implementation is based on:

```python
BaseCoordinator
```

Each connector must implement its own assignment strategy.

---

## Gateway Responsibilities

A Gateway is responsible for:

* Discovering the Coordinator.
* Registering itself with the Coordinator.
* Connecting assigned devices.
* Disconnecting devices when requested.
* Rebuilding state after restart.
* Maintaining device-specific resources and connections.

The Gateway implementation is based on:

```python
BaseGateway
```

Each connector must implement device-specific connection logic.

---

## Assignment Persistence

Assignments are persisted in Kafka and materialized into ksqlDB tables.

### Assignment Source Table

```text
<CONNECTOR_NAME>_DEVICE_ASSIGNMENT_SOURCE
```

Stores assignment events.

Fields:

* DEVICE_UUID
* GATEWAY_UUID

### Assignment Table

```text
<CONNECTOR_NAME>_DEVICE_ASSIGNMENT
```

Materialized lookup table used by both Coordinator and Gateways.

This persistence layer allows recovery after service restarts.

---

## Startup Sequence

### Coordinator Startup

1. Initialize OpenFactory application.
2. Create assignment tables if necessary.
3. Discover existing gateways.
4. Register Prometheus metrics.
5. Begin accepting device assignment requests.

### Gateway Startup

1. Initialize OpenFactory application.
2. Discover the Coordinator.
3. Wait for Coordinator availability.
4. Register with the Coordinator.
5. Wait for assignment tables to exist.
6. Rebuild local state from ksqlDB.
7. Resume normal operation.

---

## OpenFactory Methods

### Coordinator Methods

#### Register a Device

```python
register_device(device_config: str)
```

Assigns a device to a gateway and records the assignment.

#### Deregister a Device

```python
deregister_device(device_uuid: str)
```

Removes a device assignment and instructs the gateway to disconnect it.

#### Register a Gateway

```python
register_gateway(gateway_uuid: str)
```

Registers a gateway with the Coordinator.

---

### Gateway Methods

#### Register a Device

```python
register_device(device_config: str)
```

Connects and initializes a device.

#### Deregister a Device

```python
deregister_device(device_uuid: str)
```

Disconnects and removes a device.

---

## Creating a New Connector

To create a new connector, implement subclasses of both `BaseCoordinator` and `BaseGateway`.

### Coordinator Example

```python
from openfactory.connectors import BaseCoordinator

class ModbusCoordinator(BaseCoordinator):

    CONNECTOR_NAME = "Modbus"

    def assign_gateway(self):
        return self.gateways[0]
```

### Gateway Example

```python
from openfactory.connectors import BaseGateway

class ModbusGateway(BaseGateway):

    CONNECTOR_NAME = "Modbus"

    def connect_device(self, device):
        pass

    def disconnect_device(self, device_uuid):
        pass
```

The framework handles discovery, registration, persistence, and recovery automatically.

---

## Recovery and Fault Tolerance

The framework is designed to recover automatically from common failures.

### Coordinator Restart

Assignments remain stored in Kafka and ksqlDB.

After restart the Coordinator rebuilds its view of available gateways and continues operation.

### Gateway Restart

Assigned devices are automatically reloaded from ksqlDB and reconnected.

### Container Recreation

Because assignments are persisted externally, container replacement does not result in assignment loss.

### Cluster Rescheduling

Services can be moved between nodes without requiring manual reassignment of devices.

---

## Metrics

Both Coordinators and Gateways expose Prometheus metrics through:

```text
/metrics
```

Metrics include:

* Build information
* Deployment metadata
* Device counts
* Assignment counts
* Assignment latency

Connector implementations may expose additional metrics.

---

## Environment Variables

| Variable              | Description                     | Required |
| --------------------- | ------------------------------- | -------- |
| `KAFKA_BROKER`        | Kafka bootstrap servers         | Yes      |
| `KSQLDB_URL`          | ksqlDB server URL               | Yes      |
| `APPLICATION_VERSION` | Application version for metrics | Optional |
| `LOG_HTTP_REQUESTS`   | Enables HTTP request logging when set to `1`, `true`, `yes`, or `on`  case-insensitive). Defaults to `false`. | Optional |
| `NODE_HOSTNAME`       | Node hostname for metrics       | Optional |

---

## Deployment

The framework is intended to run as OpenFactory applications.

Typical deployments include:

* Docker Compose
* Docker Swarm

Multiple Gateway instances may be deployed simultaneously to increase capacity and provide horizontal scalability.
