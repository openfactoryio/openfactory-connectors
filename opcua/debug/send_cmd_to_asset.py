import openfactory.config as config
from openfactory.assets import Asset
from openfactory.kafka import KSQLDBClient


ksql = KSQLDBClient(config.KSQLDB_URL)

temp_sensor = Asset('OPCUA-SENSOR-001', ksqlClient=ksql)
print("Requesting Calibrate method ...")
temp_sensor.Calibrate_CMD = ""

barcode_reader = Asset('VIRTUAL-BARCODE-READER', ksqlClient=ksql)
print("Requesting SetManualMode method ...")
barcode_reader.SetManualMode_CMD = ""
print("Requesting GenerateCode method ...")
barcode_reader.GenerateCode_CMD = "123456789"
