# Debug tools

## Deploy some virtual OPC UA devices:
Deploy the virtual OPC UA devices with
```bash
docker run -d --name virtual-opcua-sensor -p 4840:4840 -e NUM_SENSORS=2 ghcr.io/openfactoryio/virtual-opcua-sensor
```
