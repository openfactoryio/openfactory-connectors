import asyncio
import os
from datetime import datetime, timezone
from openfactory.kafka import KSQLDBClient
from openfactory.schemas.devices import Device
from openfactory.assets import AssetAttribute
from openfactory.assets.utils import current_timestamp
from connectors.common.gateway import BaseGateway
from connectors.common.kafka_producer import GlobalAssetProducer


class SHDRGateway(BaseGateway):

    CONNECTOR_NAME = "SHDR"

    # active device runtimes
    device_tasks: dict[str, asyncio.Task] = {}

    # device configs
    devices: dict[str, Device] = {}

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # Global Kafka Producer
        self.global_producer = GlobalAssetProducer(ksqlClient=self.ksql)

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
        if device.uuid in self.device_tasks:
            self.logger.warning(f"Device {device.uuid} already connected")
            return

        self.devices[device.uuid] = device
        task = asyncio.create_task(self._device_loop(device))
        self.device_tasks[device.uuid] = task

    def disconnect_device(self, device_uuid: str):
        """
        Disconnect a device

        Args:
            device_uuid (str): The UUID of the device to disconnect.
        """
        self.logger.info(f"Disconnecting device {device_uuid}")

        task = self.device_tasks.pop(device_uuid, None)
        if task is None:
            self.logger.warning(f"Device {device_uuid} had no active task associated.")
            return

        task.cancel()
        self.devices.pop(device_uuid, None)

    def _utc_now_iso(self) -> str:
        """ Return current UTC timestamp in ISO-8601 format. """
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def parse_shdr_line(self, line: str) -> dict:
        """
        Parse a simple SHDR line.

        Example:
            2026-05-28T00:10:00Z|temp|42.1|humi|71

        Returns:
            {
                "timestamp": "2026-05-28T00:10:00Z",
                "values": {
                    "temp": "42.1",
                    "humi": "71"
                }
            }
        """
        line = line.strip()
        if line.startswith("*"):
            return {"command": line[1:].strip()}

        parts = [p.strip() for p in line.strip().split("|")]
        if len(parts) < 3:
            raise ValueError("Invalid SHDR line")

        timestamp = parts[0] or self._utc_now_iso()
        kv = parts[1:]
        if len(kv) % 2 != 0:
            raise ValueError("Malformed SHDR key/value pairs")

        values = {}
        for i in range(0, len(kv), 2):
            key = kv[i]
            if not key:
                raise ValueError("SHDR keys cannot be empty")
            values[key] = kv[i + 1]

        return {
            "timestamp": timestamp,
            "values": values
        }

    async def _device_loop(self, device: Device):

        self.logger.info(f"Starting SHDR monitoring task for {device.uuid}")
        connector = device.connector

        while True:
            reader = None
            writer = None

            try:
                self.logger.info(f"Connecting to {device.uuid} ({connector.host}:{connector.port})")
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(str(connector.host), connector.port), timeout=5)
                self.logger.info(f"Connected to {device.uuid}")
                self.global_producer.send(
                    asset_uuid=device.uuid,
                    asset_attribute=AssetAttribute(
                        id='avail',
                        value='AVAILABLE',
                        type='Events',
                        tag='Availability'
                        )
                    )

                while True:
                    line = await reader.readline()
                    if not line:
                        # socket closed
                        self.logger.warning("SHDR stream closed")
                        break

                    line = line.decode().strip()
                    parsed = self.parse_shdr_line(line)
                    if "command" in parsed:
                        self.logger.debug(f"Received SHDR command '{parsed['command']}'")
                        continue
                    timestamp = parsed["timestamp"]
                    for key, value in parsed["values"].items():
                        datapoint = connector.data.get(key)
                        if datapoint is None:
                            self.logger.warning(f"Unknown SHDR key '{key}' for device {device.uuid}")
                            continue
                        self.global_producer.send(
                            asset_uuid=device.uuid,
                            asset_attribute=AssetAttribute(
                                id=key,
                                value=value,
                                type=datapoint.type,
                                tag=datapoint.tag,
                                timestamp=timestamp
                                ),
                            ingestion_timestamp=current_timestamp()
                            )
                        self.logger.debug(f"[{device.uuid}] ({timestamp}) {key}={value} tag={datapoint.tag}")

            except asyncio.CancelledError:
                self.logger.debug(f"Stopping SHDR monitoring task for {device.uuid}")
                raise

            except Exception as e:
                self.logger.warning(f"{device.uuid} disconnected: {e}")
                self.global_producer.send(
                    asset_uuid=device.uuid,
                    asset_attribute=AssetAttribute(
                        id='avail',
                        value='UNAVAILABLE',
                        type='Events',
                        tag='Availability'
                        )
                    )
                await asyncio.sleep(5)

            finally:
                if writer is not None:
                    self.logger.debug(f"Closing TCP connection to {device.uuid}")
                    self.global_producer.send(
                        asset_uuid=device.uuid,
                        asset_attribute=AssetAttribute(
                            id='avail',
                            value='UNAVAILABLE',
                            type='Events',
                            tag='Availability'
                            )
                        )
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                self.logger.info(f"Stopped SHDR monitoring task for {device.uuid}")


if __name__ == "__main__":
    app = SHDRGateway(
        ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        loglevel=os.getenv("LOG_LEVEL", "INFO")
    )

    app.run()
