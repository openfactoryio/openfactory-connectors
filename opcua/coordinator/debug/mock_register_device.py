"""
Simple script to register devices with OPCUA-Coordinator for debugging.

- Reads an OpenFactory device YAML file
- Validates using existing Device schema
- Sends registration request to the Coordinator API
"""

import requests
from openfactory.schemas.uns import UNSSchema
from openfactory.schemas.devices import get_devices_from_config_file


# ----------------------------
# CONFIG
# ----------------------------
uns_config = "uns.yml"
yaml_file = "device.yml"
coordinator_url = "http://localhost:8000"

# ----------------------------
# Load & validate devices
# ----------------------------
uns_schema = UNSSchema(schema_yaml_file=uns_config)
devices = get_devices_from_config_file(yaml_file, uns_schema)

if not devices:
    print("Failed to load devices. Check YAML configuration.")
    exit(1)

# ----------------------------
# Send registration requests
# ----------------------------
for device_name, device in devices.items():
    url = f"{coordinator_url}/register_device"
    payload = {"device": device.model_dump()}
    print(f"Registering device '{device_name}' at {url} ...")
    try:
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        print("✅ Success:", resp.json())
    except Exception as e:
        print("❌ Failed:", e)
