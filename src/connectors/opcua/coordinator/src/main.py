
"""
Start the OPC UA Coordinator for OpenFactory.

The coordinator manages which OPC UA gateway is responsible
for each connected device. It uses round-robin assignment
to distribute devices across gateways.
"""

import os

from openfactory.kafka import KSQLDBClient
from connectors.common.coordinator import BaseCoordinator
from connectors.common.gateway_assignment.round_robin import RoundRobinGatewayAssignmentMixin


class OPCUACoordinator(RoundRobinGatewayAssignmentMixin, BaseCoordinator):
    """ Manage OPC UA gateway assignments using round-robin selection. """

    CONNECTOR_NAME = "OPCUA"


# Create the coordinator using the OpenFactory connection settings.
app = OPCUACoordinator(
    ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    loglevel=os.getenv("LOG_LEVEL", "DEBUG")
)

app.run()
