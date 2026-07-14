import os
import openfactory.config as config
from openfactory.assets import Asset
from openfactory.kafka import KSQLDBClient

ksql = KSQLDBClient(config.KSQLDB_URL)

# Example: temp sensor
temp_sensor = Asset('OPCUA-SENSOR-002', ksqlClient=ksql, bootstrap_servers=os.getenv("KAFKA_BROKER"))

temp_sensor.temp_unit = 'K'
