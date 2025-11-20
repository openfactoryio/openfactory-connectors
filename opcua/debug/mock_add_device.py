"""
Simple script to add devices to an OPCUA-Gateway for debugging.

- Reads an OpenFactory device YAML file
- Validates using existing Device schema
- Sends registration request to the Gateway API
"""

import os
import requests
from openfactory.schemas.uns import UNSSchema
from openfactory.schemas.devices import get_devices_from_config_file


# ----------------------------
# CONFIG
# ----------------------------
host_ip = os.getenv("CONTAINER_IP")
uns_config = "uns.yml"
yaml_file = "device.yml"
gateway_url = f"http://{host_ip}:8001"

# ----------------------------
# Load & validate devices
# ----------------------------
uns_schema = UNSSchema(schema_yaml_file=uns_config)
devices = get_devices_from_config_file(yaml_file, uns_schema)

if not devices:
    print("Failed to load devices. Check YAML configuration.")
    exit(1)

# ----------------------------
# Send add requests
# ----------------------------
for device_name, device in devices.items():
    url = f"{gateway_url}/add_device"
    payload = {"device": device.model_dump()}
    print(f"Adding device '{device_name}' at {url} ...")
    try:
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        print("✅ Success:", resp.json())
    except Exception as e:
        print("❌ Failed:", e)
