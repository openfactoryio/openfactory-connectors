import os
from uuid import uuid4
from datetime import datetime, timezone
import openfactory.config as config
from openfactory.assets import Asset
from openfactory.kafka import KSQLDBClient
from openfactory.schemas.command_header import CommandEnvelope, CommandHeader

ksql = KSQLDBClient(config.KSQLDB_URL)

# Example 1: temp sensor
temp_sensor = Asset('OPCUA-SENSOR-001', ksqlClient=ksql, bootstrap_servers=os.getenv("KAFKA_BROKER"))
print("Requesting Calibrate method ...")
envelope = CommandEnvelope(
    header=CommandHeader(
        correlation_id=uuid4(),
        sender_uuid="TEST-HMI",
        timestamp=datetime.now(timezone.utc),
        signature=None
    ),
    arguments={}
)
temp_sensor.Calibrate_CMD = envelope.model_dump_json()

# Example 2: barcode reader - SetManualMode
barcode_reader = Asset('VIRTUAL-BARCODE-READER', ksqlClient=ksql, bootstrap_servers=os.getenv("KAFKA_BROKER"))
print("Requesting SetManualMode method ...")
envelope = CommandEnvelope(
    header=CommandHeader(
        correlation_id=uuid4(),
        sender_uuid="TEST-HMI",
        timestamp=datetime.now(timezone.utc),
        signature=None
    ),
    arguments={}
)
barcode_reader.SetManualMode_CMD = envelope.model_dump_json()

# Example 3: barcode reader - GenerateCode with argument
cmd_args = {"Code": "123456789"}
envelope = CommandEnvelope(
    header=CommandHeader(
        correlation_id=uuid4(),
        sender_uuid="TEST-HMI",
        timestamp=datetime.now(timezone.utc),
        signature=None,
    ),
    arguments=cmd_args
)
print("Requesting GenerateCode method ...")
barcode_reader.GenerateCode_CMD = envelope.model_dump_json()
