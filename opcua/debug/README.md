# Debug tools
This folder contains some tools useful during development.

> Note: These codes are not always up-to date and should be considered as examples

## Deploy some virtual OPC UA devices:
Deploy the virtual OPC UA devices with
```bash
docker run -d --name virtual-opcua-sensor -p 4840:4840 -e NUM_SENSORS=2 ghcr.io/openfactoryio/virtual-opcua-sensor
docker run -d -p 4841:4840 --name virtual-opcua-barcode-reader ghcr.io/openfactoryio/virtual-opcua-barcode-reader
```
and then register them with OpenFactory:
```bash
ofa device up debug/dev_temp_sensors.yml
ofa device up debug/dev_barcode_reader.yml
```

## Verify OpenFactory pipeline
The code `subscribe_to_assets.py` subscribes to the assets deployed above:
```bash
python debug/subscribe_to_assets.py
```
