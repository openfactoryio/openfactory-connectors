import asyncio
import os
from openfactory.kafka import KSQLDBClient
from openfactory.schemas.devices import Device
from connectors.common.gateway import BaseGateway
from connectors.common.kafka_producer import GlobalAssetProducer
from .monitor import DeviceMonitor


class OPCUAGateway(BaseGateway):
    """
    OPC UA gateway implementation.

    Manages OPC UA device registrations and creates a dedicated
    monitoring task for each configured device. Monitoring tasks
    are responsible for maintaining OPC UA sessions, collecting
    data, exposing commands, and forwarding updates to OpenFactory.

    Attributes:
        device_tasks (dict[str, asyncio.Task]):
            Mapping of device UUIDs to active monitoring tasks.

        devices (dict[str, Device]):
            Mapping of device UUIDs to registered device configurations.
    """

    CONNECTOR_NAME = "OPCUA"

    # active device runtimes
    device_tasks: dict[str, asyncio.Task] = {}

    # device configs
    devices: dict[str, Device] = {}

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # Global Kafka Producer
        self.global_producer = GlobalAssetProducer(ksqlClient=self.ksql)

    async def monitor_device(self, device: Device):
        """
        Run the monitoring lifecycle for a single OPC UA device.

        This task creates a DeviceMonitor and delegates all device-specific
        monitoring responsibilities, including:

        - Establishing and maintaining OPC UA sessions
        - Discovering OPC UA methods and exposing OpenFactory commands
        - Reading configured constants
        - Subscribing to variables and events
        - Forwarding asset updates through the global Kafka producer
        - Handling reconnects and clean shutdowns

        Args:
            device (Device): Device schema instance defining OPC UA connector settings.
        """
        try:
            monitor = DeviceMonitor(device, self)
            self.logger.debug(f"Starting monitoring task {device.uuid}")
            await monitor.run()

        except asyncio.CancelledError:
            raise

        except Exception:
            self.logger.error(
                f"Unhandled exception while monitoring device {device.uuid}",
                exc_info=True
            )

    def connect_device(self, device: Device):
        """
        Register a device and start its monitoring task.

        Validates that the device uses the OPC UA connector, creates an
        asynchronous monitoring task, and stores the task and device
        references for lifecycle management.

        Args:
            device (Device): Device to monitor.
        """
        if device.connector.type != 'opcua':
            self.logger.warning(f"Device {device.uuid} is not an OPC UA device.")
            self.logger.warning(f"Abort registration of {device.uuid}.")
            return
        if device.uuid in self.device_tasks:
            self.logger.warning(f"Device {device.uuid} already connected")
            return

        self.devices[device.uuid] = device

        try:
            task = asyncio.create_task(self.monitor_device(device))
            task.add_done_callback(
                lambda t: self.logger.error(f"Task for {device.uuid} terminated with {t.exception()!r}")
                if not t.cancelled() and t.exception()
                else None
            )
        except Exception as e:
            self.logger.warning(f"Failed to connect device {device.uuid}: {e}", exc_info=True)
            return

        self.device_tasks[device.uuid] = task

    def disconnect_device(self, device_uuid: str):
        """
        Stop monitoring a device.

        Cancels the device monitoring task and removes the device from
        the internal registries.

        Args:
            device_uuid (str): UUID of the device to disconnect.
        """
        self.logger.info(f"Disconnecting device {device_uuid}")

        task = self.device_tasks.pop(device_uuid, None)
        if task is None:
            self.logger.warning(f"Device {device_uuid} had no active task associated.")
            return

        task.cancel()
        self.devices.pop(device_uuid, None)


app = OPCUAGateway(
    ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    loglevel=os.getenv("LOG_LEVEL", "INFO")
)

app.run()
