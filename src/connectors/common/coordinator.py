import json
from typing import Annotated
from openfactory.apps import OpenFactoryFastAPIApp, ofa_method
from openfactory.assets import Asset
from openfactory.schemas.devices import Device


class BaseCoordinator(OpenFactoryFastAPIApp):

    CONNECTOR_NAME: str | None = None
    gateways = []

    def __init__(self, *args, **kwargs):
        """
        Initialize the BaseCoordinator.

        This constructor forwards all parameters to
        :class:`OpenFactoryFastAPIApp`

        Args:
            ksqlClient: KSQL client instance.
            bootstrap_servers: Kafka bootstrap server address.
            asset_router_url: Asset Router URL.
            loglevel: Logging level (e.g., ``INFO``, ``DEBUG``).
            test_mode: Enables test mode (disables live Kafka/ksql interaction).

        See also:
            :class:`OpenFactoryFastAPIApp` for full initialization
            details and environment variable handling.
        """
        if self.CONNECTOR_NAME is None:
            raise NotImplementedError(f"{self.__class__.__name__} must define CONNECTOR_NAME")

        super().__init__(*args, **kwargs)

        # expose OFA app inside FastAPI
        self.api.state.ofa_app = self

        # redefine the Asset type
        self.wait_until(attribute_id='AssetType', value='OpenFactoryApp')
        self.AssetType = f'{self.CONNECTOR_NAME}.Coordinator'

        self.create_device_assignment_tables()
        self.discover_gateways()

    def create_device_assignment_tables(self):
        """ Ensure that the KSQLDB tables for device assignment exists. """
        self.logger.info(f"Creating {self.CONNECTOR_NAME} assignment tables if they do not exist.")

        # Source tables
        self.ksql.statement_query(f"""
        CREATE TABLE IF NOT EXISTS {self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT_SOURCE (
            DEVICE_UUID STRING PRIMARY KEY,
            GATEWAY_UUID STRING
        ) WITH (
            KAFKA_TOPIC='{self.CONNECTOR_NAME.lower()}_device_assignment_topic',
            VALUE_FORMAT='JSON',
            PARTITIONS=1
        );
        """)

        # Materialized tables
        self.ksql.statement_query(f"""
        CREATE TABLE IF NOT EXISTS {self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT AS
            SELECT DEVICE_UUID, GATEWAY_UUID
            FROM {self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT_SOURCE
            EMIT CHANGES;
        """)

    def discover_gateways(self):
        """ Discover all deployed gateways """
        self.logger.info("Discovering deployed gateways")
        query = f"select ASSET_UUID from ASSETS_TYPE where TYPE='{self.CONNECTOR_NAME}.Gateway';"
        gateways = self.ksql.query(query)
        for gateway in gateways:
            self.register_gateway(gateway['ASSET_UUID'])
        self.logger.info(f"Discovered all deployed gateways: {str(self.gateways)}")

    def assign_gateway(self) -> str:
        """
        Assign a gateway using a round-robin strategy.
        Children must override this class
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement assign_gateway()")

    def get_assigned_gateway_uuid(self, device_uuid: str) -> tuple[str, str] | None:
        """
        Return the Gateway host to which a device is assigned.

        Args:
            device_uuid (str): Device UUID to look up.

        Returns:
            str: Gateway UUID to which a device is assigned or None if not assigned to any.
        """
        rows = self.ksql.query(
            f"SELECT GATEWAY_UUID FROM {self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT WHERE DEVICE_UUID='{device_uuid}';"
            )
        if not rows:
            return None
        return rows[0]["GATEWAY_UUID"]

    @ofa_method(description="Register a device")
    def register_device(
        self,
        device_config: Annotated[str, "Device configuration"],
    ):
        try:
            cfg = json.loads(device_config)
            device = Device(**cfg)
        except Exception as e:
            self.logger.warning(f"Failed to register device {device_config}: {e}", exc_info=True)
            return
        gateway_uuid = self.assign_gateway()
        self.logger.info(f"Registering new device {device.uuid} with Gateway '{gateway_uuid}'")
        self.logger.debug(f"Config {device_config}")
        gateway = Asset(asset_uuid=gateway_uuid, ksqlClient=self.ksql)
        if gateway.avail.value != "AVAILABLE":
            self.logger.warning(f"Gateway '{gateway.asset_uuid}' is not AVAILABLE")
            self.logger.warning(f"Failed to register device {device_config}")
        else:
            try:
                gateway.register_device(sender_uuid=self.asset_uuid, device_config=device_config)
                self.ksql.insert_into_stream(f"{self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT_SOURCE",
                                             [{"DEVICE_UUID": device.uuid, "GATEWAY_UUID": gateway.asset_uuid}])
            except TypeError:
                self.logger.warning(f"Asset '{gateway.asset_uuid}' does not appear to be a valid gateway.")
                self.logger.warning(f"Failed to register device {device.uuid}")
            except Exception as e:
                self.logger.error(f"Failed to record assignment of {device.uuid} in KSQLDB: {e}")
        gateway.close()

    @ofa_method(description="Deregister a device")
    def deregister_device(
        self,
        device_uuid: Annotated[str, "Device UUID"],
    ):
        gateway_uuid = self.get_assigned_gateway_uuid(device_uuid)
        self.logger.info(f"Deregister device {device_uuid} from Gateway '{gateway_uuid}'")
        if not gateway_uuid:
            self.logger.warning("Aborting deregistration as no associated Gateway was found")
            return
        gateway = Asset(asset_uuid=gateway_uuid, ksqlClient=self.ksql)
        if gateway.avail.value != "AVAILABLE":
            self.logger.warning(f"Gateway '{gateway.asset_uuid}' is not AVAILABLE")
            self.logger.warning(f"Failed to deregister device {device_uuid}")
        try:
            gateway.deregister_device(sender_uuid=self.asset_uuid, device_uuid=device_uuid)
        except TypeError:
            self.logger.warning(f"Asset '{gateway.asset_uuid}' does not appear to be a valid gateway.")
            self.logger.warning(f"Failed to deregister device {device_uuid}")

    @ofa_method(description="Register a Gateway")
    def register_gateway(
        self,
        gateway_uuid: Annotated[str, "Gateway UUID"],
    ):
        self.logger.info(f"Registering new gateway {gateway_uuid}")
        if gateway_uuid not in self.gateways:
            self.gateways.append(gateway_uuid)
