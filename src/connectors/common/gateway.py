import json
import time
import asyncio
import os
from typing import Annotated
from openfactory.exceptions import OFAException
from openfactory.apps import OpenFactoryFastAPIApp, ofa_method
from openfactory.schemas.devices import Device
from openfactory.assets import Asset, AssetAttribute
from openfactory.apps.attributefield import SampleAttribute, EventAttribute
from . import gateway_metrics

PROMETHEUS_METRICS_PATH = "/metrics"


class BaseGateway(OpenFactoryFastAPIApp):

    CONNECTOR_NAME: str | None = None

    device_count = SampleAttribute(value=0, tag='Device.Count')
    prometheus_metrics_path = EventAttribute(value=PROMETHEUS_METRICS_PATH, tag="Prometheus.metrics_path")

    _device_count = 0

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

        super().__init__(*args, **kwargs)

        # expose OFA app inside FastAPI
        self.api.state.ofa_app = self

        # redefine the Asset type
        if not getattr(self, "_test_mode", False):
            self.wait_until(attribute_id='AssetType', value='OpenFactoryApp')
        self.AssetType = f'{self.CONNECTOR_NAME}.Gateway'

        # discover coordinator
        self._discover_coordinator()

        # wait coordinator becomes available
        self.coordinator = Asset(asset_uuid=self.COORDINATOR_UUID, ksqlClient=self.ksql)
        self._wait_coordinator_available()

        # register gateway with coordinator
        self.register_gateway()

        # Coordinator build info metrics
        gateway_metrics.BUILD_INFO.info({
            "version": os.environ.get('APPLICATION_VERSION', 'UNKNOWN'),
            "swarm_node": os.environ.get('NODE_HOSTNAME', 'unknown'),
        })

        # Expose Prometheus metrics
        self.api.get(PROMETHEUS_METRICS_PATH)(gateway_metrics.metrics_endpoint)

    @property
    def assignment_source_table(self) -> str:
        return f"{self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT_SOURCE"

    @property
    def assignment_table(self) -> str:
        return f"{self.CONNECTOR_NAME}_DEVICE_ASSIGNMENT"

    @property
    def assignment_topic(self) -> str:
        return f"{self.CONNECTOR_NAME.lower()}_device_assignment_topic"

    def connect_device(self, device: Device):
        """
        Connects a device

        Children must override this class

        Args:
            device (Device): The device to connect to.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement connect_device()")

    def disconnect_device(self, device_uuid: str):
        """
        Disconnect a device.

        Children must override this method.

        Args:
            device_uuid (str): The UUID of the device to disconnect.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement disconnect_device()")

    def _discover_coordinator(self) -> None:
        """ Discover connector coordinator """
        self.logger.info(f"Discovering {self.CONNECTOR_NAME} coordinator")
        query = f"select ASSET_UUID FROM ASSETS_TYPE WHERE TYPE='{self.CONNECTOR_NAME}.Coordinator';"
        while True:
            res = self.ksql.query(query)
            if res:
                self.COORDINATOR_UUID = res[0]["ASSET_UUID"]
                self.logger.info(f"Found {self.CONNECTOR_NAME} coordinator {self.COORDINATOR_UUID}")
                break

            self.logger.debug(f"{self.CONNECTOR_NAME} coordinator not found yet")
            time.sleep(1)

    def _wait_coordinator_available(self, timeout: float | None = None) -> None:
        """
        Wait for connector coordinator to become available.

        Args:
            timeout: Maximum time to wait in seconds. If None, wait forever.
        """
        self.logger.info(f"Waiting for coordinator {self.COORDINATOR_UUID} to become available...")

        start_time = time.time()
        while True:
            if self.coordinator.wait_until(attribute_id="avail", value="AVAILABLE", timeout=30):
                self.logger.info(f"Coordinator {self.COORDINATOR_UUID} is available.")
                return

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise OFAException(f"Coordinator {self.COORDINATOR_UUID} did not become available within {timeout} seconds.")

            self.logger.info(f"Coordinator {self.COORDINATOR_UUID} not yet available")

    def _fetch_assigned_devices(self):
        query = f"""
        SELECT DEVICE_UUID
        FROM {self.assignment_table}
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
        """ Register the Gateway with the coordinator. """
        self.logger.info("Registering gateway with coordinator")
        try:
            self.coordinator.register_gateway(sender_uuid=self.asset_uuid, gateway_uuid=self.asset_uuid)
        except TypeError:
            raise OFAException(f"Asset '{self.COORDINATOR_UUID}' does not appear to be a valid coordinator.")

    def wait_for_existence_of_tables(self):
        """ Wait until all tables created by coordinator exist. """
        tables_to_check = {
            self.assignment_source_table,
            self.assignment_table,
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
                self.register_device(raw_config)
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
            asset = Asset(device.uuid, ksqlClient=self.ksql)
            asset.add_attribute(
                AssetAttribute(
                    id='gateway',
                    type='Events',
                    tag=f'{self.CONNECTOR_NAME}.Gateway',
                    value=self.asset_uuid
                )
            )
            asset.close()
            self.connect_device(device)
            self._device_count = self._device_count + 1
            self.device_count = self._device_count
            gateway_metrics.GATEWAY_DEVICE_COUNT.set(self._device_count)
        except Exception as e:
            self.logger.warning(f"Failed to connect device {device_config}: {e}", exc_info=True)

    @ofa_method(description="Deregister a device")
    def deregister_device(
        self,
        device_uuid: Annotated[str, "Device UUID"],
    ):
        self.logger.info(f"Deregistering device {device_uuid}")
        self.disconnect_device(device_uuid)
        self._device_count = self._device_count - 1
        self.device_count = self._device_count
        gateway_metrics.GATEWAY_DEVICE_COUNT.set(self._device_count)

    async def async_main_loop(self):
        """ asynchronous main loop """

        # wait for all ksqlDB tables created by coordinator to be ready
        self.wait_for_existence_of_tables()

        # rebuild state
        self.rebuild_gateway_state()

        while True:
            await asyncio.sleep(3600)
