import time
from datetime import datetime, timezone
import openfactory.config as config
from openfactory.assets import Asset
from openfactory.kafka import KSQLDBClient


ksql = KSQLDBClient(config.KSQLDB_URL)
asset = Asset('OPCUA-SENSOR-002', ksqlClient=ksql)


def on_sample(msg_subject, msg_value):
    # Current time
    now_iso = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    # Extract timestamp from message
    msg_ts = msg_value['attributes']['timestamp']

    # Compute time difference in seconds
    msg_time = datetime.fromisoformat(msg_ts.replace('Z', '+00:00'))
    now_time = datetime.fromisoformat(now_iso.replace('Z', '+00:00'))
    delta = (now_time - msg_time).total_seconds()

    print(f"[{msg_subject}] {msg_value} latency={1000*delta:.1f}ms")


print("Running ...")
asset.subscribe_to_samples(on_sample)

time.sleep(10)
asset.close()
