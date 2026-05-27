import os
from openfactory.kafka import KSQLDBClient
from connectors.common.gateway import BaseGateway
from openfactory.schemas.devices import Device


class SHDRGateway(BaseGateway):

    CONNECTOR_NAME = "SHDR"
    COORDINATOR_UUID = "SHDR-COORDINATOR"

    def connect_device(self, device: Device):
        """
        Connects a device

        Args:
            device (Device): The device to connect to.
        """
        if device.connector.type != 'shdr':
            self.logger.warning(f"Device {device.uuid} is not an SHDR device.")
            self.logger.warning(f"Abort registration of {device.uuid}.")
            return
        self.logger.info(f"Connecting device {device.uuid}")

    def deconnect_device(self, device_uuid: str):
        """
        Deconnects a device

        Args:
            device (Device): The device to deconnect.
        """
        self.logger.info(f"Deconnecting device {device_uuid}")


app = SHDRGateway(
    ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    loglevel=os.getenv("LOG_LEVEL", "DEBUG")
)

app.run()
