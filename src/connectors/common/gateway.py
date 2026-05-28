import json
import time
import asyncio
from typing import Annotated
from openfactory.exceptions import OFAException
from openfactory.apps import OpenFactoryFastAPIApp, ofa_method
from openfactory.schemas.devices import Device
from openfactory.assets import Asset


class BaseGateway(OpenFactoryFastAPIApp):

    CONNECTOR_NAME: str | None = None
    COORDINATOR_UUID: str | None = None

    def __init__(self, *args, **kwargs):
        """
        Initialize the BaseGateway.

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
        if self.COORDINATOR_UUID is None:
            raise NotImplementedError(f"{self.__class__.__name__} must define COORDINATOR_UUID")

        super().__init__(*args, **kwargs)

        # expose OFA app inside FastAPI
        self.api.state.ofa_app = self

        # redefine the Asset type
        self.wait_until(attribute_id='AssetType', value='OpenFactoryApp')
        self.AssetType = f'{self.CONNECTOR_NAME}.Gateway'

        # register gateway with coordinator
        self.coordinator = Asset(asset_uuid=self.COORDINATOR_UUID, ksqlClient=self.ksql)
        if not self.coordinator.wait_until(attribute_id='avail', value="AVAILABLE", timeout=300):
            self.logger.error(f"Coordinator {self.COORDINATOR_UUID} is not deployed")
            raise OFAException(f"Coordinator {self.COORDINATOR_UUID} is not deployed")
        self.register_gateway()

    def connect_device(self, device: Device):
        """
        Connects a device

        Children must override this class

        Args:
            device (Device): The device to connect to.
        """
        pass

    def deconnect_device(self, device_uuid: str):
        """
        Deconnects a device

        Children must override this class

        Args:
            device (Device): The device to deconnect.
        """
        pass

    def _get_coordinator(self) -> Asset:
        """
        Get the coordinator asset.

        Returns:
            Asset: The coordinator asset.

        Raises:
            OFAException: If the SHDR coordinator is unavailable.
        """
        coordinator = Asset(asset_uuid=self.COORDINATOR_UUID, ksqlClient=self.ksql)
        ret = coordinator.wait_until(attribute_id='avail', value="AVAILABLE", timeout=120)
        if not ret:
            self.logger.error(f"Coordinator {self.COORDINATOR_UUID} is not deployed")
            raise
        return coordinator

    def _fetch_assigned_devices(self):
        query = f"""
        SELECT DEVICE_UUID
        FROM {self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT
        WHERE GATEWAY_UUID = '{self.asset_uuid}';
        """
        return self.ksql.query(query)

    def _fetch_device_configs(self, device_uuids: list[str]):
        if not device_uuids:
            return []
        uuids_str = ",".join(f"'{u}'" for u in device_uuids)
        query = f"""
        SELECT ASSET_UUID, CONNECTOR_CONFIG
        FROM DEVICE_CONNECTOR
        WHERE ASSET_UUID IN ({uuids_str});
        """
        return self.ksql.query(query)

    def register_gateway(self):
        """ Register the Gateway with the coordiantor. """
        try:
            self.coordinator.register_gateway(sender_uuid=self.asset_uuid, gateway_uuid=self.asset_uuid)
        except TypeError:
            raise OFAException(f"Asset '{self.COORDINATOR_UUID}' does not appear to be a valid coordinator.")

    def wait_for_existience_of_tables(self):
        """ Wait until all tables ceated by coordiantor exist. """
        tables_to_check = {
            f"{self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT_SOURCE",
            f"{self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT",
        }

        while True:
            existing_tables = set(self.ksql.tables())
            missing_tables = tables_to_check - existing_tables

            if not missing_tables:
                return

            self.logger.info(
                f"Waiting for ksqlDB tables to become available: "
                f"{', '.join(sorted(missing_tables))}"
            )

            time.sleep(1)

    def rebuild_gateway_state(self):
        """
        Rebuild in-memory gateway state from ksqlDB entries.
        Only devices assigned to this gateway are loaded.
        """
        # Get assigned device UUIDs
        assigned_rows = self._fetch_assigned_devices()
        assigned_uuids = [r["DEVICE_UUID"] for r in assigned_rows if r.get("DEVICE_UUID")]
        self.logger.info(f"Found {len(assigned_uuids)} devices assigned to this gateway.")

        if not assigned_uuids:
            self.logger.info("No devices assigned; nothing to rebuild.")
            return

        # Fetch their connector configs
        config_rows = self._fetch_device_configs(assigned_uuids)
        self.logger.debug(f"Fetched {len(config_rows)} connector configs from DEVICE_CONNECTOR.")

        for row in config_rows:
            dev_uuid = row.get("ASSET_UUID")
            raw_config = row.get("CONNECTOR_CONFIG")

            if not dev_uuid or not raw_config:
                self.logger.warning("Skipping malformed ksqlDB row: %s", row)
                continue

            try:
                cfg = json.loads(raw_config)
                device = Device(**cfg)
                self.connect_device(device)
            except Exception as e:
                self.logger.warning(f"Failed to connect device {dev_uuid}: {e}", exc_info=True)

    @ofa_method(description="Register a device")
    def register_device(
        self,
        device_config: Annotated[str, "Device configuration"],
    ):
        try:
            cfg = json.loads(device_config)
            device = Device(**cfg)
            self.logger.info(f"Registering device {device.uuid}")
            self.connect_device(device)
        except Exception as e:
            self.logger.warning(f"Failed to connect device {device_config}: {e}", exc_info=True)

    @ofa_method(description="Deregister a device")
    def deregister_device(
        self,
        device_uuid: Annotated[str, "Device UUID"],
    ):
        self.logger.info(f"Deregistering device {device_uuid}")
        self.deconnect_device(device_uuid)

    async def async_main_loop(self):
        """ asynchronous main loop """

        # wait for all ksqlDB tables created by coordinator to be ready
        self.wait_for_existience_of_tables()

        # rebuild state
        self.rebuild_gateway_state()

        while True:
            await asyncio.sleep(3600)
