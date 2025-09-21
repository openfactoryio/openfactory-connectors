# OpenFactory Connectors

**OpenFactory-Connectors** is a collection of protocol connectors that bridge industrial devices with the OpenFactory platform.

Each connector is responsible for collecting data from a specific protocol or ecosystem and streaming it into Kafka in a unified format.

## 🔗 Supported Connectors

- **OPC UA**  
  - `OPCUA-Coordinator`: assigns devices to gateways.  
  - `OPCUA-Gateway`: connects to one or more OPC UA servers, collects data, and streams to Kafka.

- **Planned**
    - SHDR
    - MQTT
    - Modbus
    - ...

## 📂 Repository Structure

```
openfactory-connectors/
├── opcua/          # OPC UA connectors (Coordinator + Gateway)
├── shdr/           # SHDR connector (planned)
├── mqtt/           # MQTT connector (planned)
├── modbus/         # Modbus connector (planned)
└── README.md       # General repository documentation
```

## 🚀 Usage

Each connector comes with its own README for details on setup, configuration, and deployment.  

## ⚖️ License

This project is licensed under the [Polyform Noncommercial License 1.0.0](LICENSE).
