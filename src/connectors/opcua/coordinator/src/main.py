import os
from openfactory.kafka import KSQLDBClient
from connectors.common.coordinator import BaseCoordinator
from connectors.common.gateway_assignment.round_robin import RoundRobinGatewayAssignmentMixin


class OPCUACoordinator(RoundRobinGatewayAssignmentMixin, BaseCoordinator):

    CONNECTOR_NAME = "OPCUA"


app = OPCUACoordinator(
    ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    loglevel=os.getenv("LOG_LEVEL", "DEBUG")
)

app.run()
