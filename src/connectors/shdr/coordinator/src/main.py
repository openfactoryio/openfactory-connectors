import os
from openfactory.kafka import KSQLDBClient
from connectors.common.coordinator import BaseCoordinator


class SHDRCoordinator(BaseCoordinator):

    CONNECTOR_NAME = "SHDR"


app = SHDRCoordinator(
    ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    loglevel=os.getenv("LOG_LEVEL", "DEBUG")
)

app.run()
