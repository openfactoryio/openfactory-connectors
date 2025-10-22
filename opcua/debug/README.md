# Debug tools
This folder contains some tools useful during development.

## Deploy some virtual OPC UA devices:
Deploy the virtual OPC UA devices with
```bash
docker run -d --name virtual-opcua-sensor -p 4840:4840 -e NUM_SENSORS=2 ghcr.io/openfactoryio/virtual-opcua-sensor
```
and then register them with OpenFactory:
```bash
ofa device up debug/device.yml
```

## Verify OpenFactory pipeline
The code `subscribe_to_assets.py` subscribes to the assets deployed above:
```bash
python debug/subscribe_to_assets.py
```
