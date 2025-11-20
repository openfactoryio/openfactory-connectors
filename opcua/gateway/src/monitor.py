"""
Device monitoring module for the OPC UA Gateway.

This module defines the `DeviceMonitor` class and `monitor_device` function
that manage the lifecycle of OPC UA devices. Each device monitor handles:

- Parsing the OPC UA connector schema.
- Establishing and maintaining OPC UA client sessions.
- Subscribing to variables and events.
- Forwarding asset attributes to Kafka via `GlobalAssetProducer`.
- Automatic reconnection on errors.
- Cleanup when monitoring tasks are cancelled.

The `monitor_device` function provides a cancellable entry point for running
a device monitor in an asyncio task.
"""

import asyncio
import traceback
import os
from fastapi import FastAPI
from asyncua import Client
from openfactory.schemas.devices import Device
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema
from openfactory.assets import AssetAttribute
from .subscription import SubscriptionHandler
from .state import _active_device_defs


class DeviceMonitor:
    """
    Manage the lifecycle of an OPC UA monitored device.

    Each device gets its own monitor instance, which handles:
    - Parsing connector schema
    - Establishing OPC UA sessions
    - Subscribing to variables/events
    - Updating asset attributes in Kafka/ksqlDB
    - Handling reconnects and cleanup
    """

    def __init__(self, device: Device, app: FastAPI):
        """
        Initialize monitoring of a given device.

        Args:
            device (Device): The device definition, including UUID and connector schema.
            app (FastAPI): The FastAPI application instance used to access shared state
        """
        self.device = device
        self.app = app
        self.dev_uuid = device.uuid
        self.gateway_id = app.state.gateway_id
        self.global_producer = app.state.global_producer
        self.schema = OPCUAConnectorSchema(**device.connector.model_dump())
        self.sub = None

        # Register device globally
        _active_device_defs[self.dev_uuid] = self.device

        # Logging
        self.log = app.state.logger

        # Register Producer Attributes with OpenFactory
        self.global_producer.send(
            asset_uuid=f"{self.dev_uuid}-PRODUCER",
            asset_attribute=AssetAttribute(
                id='avail',
                value="AVAILABLE",
                tag="Availability",
                type="Events"
            )
        )
        self.global_producer.send(
            asset_uuid=f"{self.dev_uuid}-PRODUCER",
            asset_attribute=AssetAttribute(
                id='application_manufacturer',
                value='OpenFactoryIO',
                type='Events',
                tag='Application.Manufacturer'
            )
        )
        self.global_producer.send(
            asset_uuid=f"{self.dev_uuid}-PRODUCER",
            asset_attribute=AssetAttribute(
                id='application_license',
                value='Polyform Noncommercial License 1.0.0',
                type='Events',
                tag='Application.License'
            )
        )
        self.global_producer.send(
            asset_uuid=f"{self.dev_uuid}-PRODUCER",
            asset_attribute=AssetAttribute(
                id='application_version',
                value=os.environ.get('APPLICATION_VERSION'),
                type='Events',
                tag='Application.Version'
            )
        )

    async def run(self) -> None:
        """
        Main entrypoint for monitoring loop.

        Keeps trying to connect and monitor the device until cancelled.
        Reconnects automatically on errors with a small backoff.
        """
        self.log.info(f"Starting monitor task for {self.dev_uuid}")
        while True:
            try:
                await self._run_session()
            except asyncio.CancelledError:
                await self._cleanup()
                raise
            except Exception as e:
                if "BadNoMatch" in str(e):
                    self.log.error(f"[{self.dev_uuid}] Device cannot be resolved with {self.schema.server.uri}: {e}")
                    await self._cleanup()
                    break
                self.log.error(f"[{self.dev_uuid}] OPC UA client error: {type(e).__name__}: {e}")
                self.log.debug(traceback.format_exc())
                try:
                    self.global_producer.send(
                        asset_uuid=self.dev_uuid,
                        asset_attribute=AssetAttribute(id='avail', value="UNAVAILABLE", tag="Availability", type="Events")
                    )
                except Exception:
                    pass
                await asyncio.sleep(2)  # backoff before reconnect

    async def _run_session(self) -> None:
        """
        Establish a session and subscribe to variables/events.

        Runs until the connection is lost. A reconnect is triggered
        by exceptions from the keepalive loop.
        """
        self.log.info(f"Connecting to OPC UA server {self.schema.server.uri}")
        async with Client(self.schema.server.uri) as client:
            handler = SubscriptionHandler(self.dev_uuid, self.app, client)
            self.sub = await client.create_subscription(
                period=float(self.schema.server.subscription.publishing_interval),
                handler=handler,
            )

            await self._subscribe_variables(client, handler)
            await self._subscribe_events()

            self.global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=AssetAttribute(id='avail', value="AVAILABLE", tag="Availability", type="Events")
            )
            self.log.info(f"[{self.dev_uuid}] Connected to OPC UA server at {self.schema.server.uri}")

            await self._keepalive_loop(client)

    async def _resolve_namespace(self, client: Client) -> int:
        """
        Resolve the namespace index from the server URI.

        Args:
            client (Client): Connected OPC UA client.

        Returns:
            int: The namespace index.

        Raises:
            Exception: If namespace resolution fails.
        """
        try:
            return await client.get_namespace_index(self.schema.server.namespace_uri)
        except Exception as e:
            self.log.error(f"Failed to resolve namespace URI {self.schema.server.namespace_uri}: {e}")
            raise

    async def _subscribe_variables(self, client: Client, handler: SubscriptionHandler) -> None:
        """
        Subscribe to all configured variables.

        Args:
            client (Client): Connected OPC UA client
            handler (SubscriptionHandler): Handles variable updates.
        """
        if not self.schema.variables:
            return
        for local_name, var_cfg in self.schema.variables.items():
            try:
                var_node = client.get_node(var_cfg.node_id)
                await self.sub.subscribe_data_change(
                    var_node,
                    queuesize=var_cfg.queue_size,
                    sampling_interval=var_cfg.sampling_interval,
                )
                handler.node_map[var_node] = {"local_name": local_name, "tag": var_cfg.tag}
                self.log.info(f"[{self.dev_uuid}] Subscribed variable {local_name} ({var_cfg.tag})")
            except Exception as e:
                self.log.error(f"[{self.dev_uuid}] Failed to subscribe variable {local_name} ({var_cfg.tag}): {e}")

    async def _subscribe_events(self) -> None:
        """ Subscribe to events. """
        if not self.schema.events:
            return
        for local_name, event_cfg in self.schema.events.items():
            try:
                await self.sub.subscribe_events(event_cfg.node_id)
                self.log.info(f"[{local_name}] Subscribed to events of node {event_cfg.node_id}")
            except Exception as e:
                self.log.error(f"[{local_name}] Failed to subscribe to events: {e}")

    async def _keepalive_loop(self, client: Client) -> None:
        """
        Keep the session alive by reading the server's Objects folder periodically.

        Args:
            client (Client): Connected OPC UA client.

        Raises:
            Exception: If the server becomes unreachable.
        """
        poll_node = client.nodes.objects
        self.log.info(f"[{self.dev_uuid}] Keepalive by polling server Objects folder")

        while True:
            await asyncio.sleep(1)
            await poll_node.read_display_name()  # exception triggers reconnect

    async def _cleanup(self) -> None:
        """
        Cleanup resources when monitor task is cancelled.

        Ensures asset availability is set to UNAVAILABLE, deletes the
        subscription, and removes device from global registries.
        """
        self.log.info(f"[{self.dev_uuid}] Monitor task cancelled; cleaning up")
        try:
            self.global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=AssetAttribute(id='avail', value="UNAVAILABLE", tag="Availability", type="Events")
            )
            self.log.info(f"[{self.dev_uuid}] Sent UNAVAILABLE status to OpenFactory")
        except Exception as e:
            self.log.error(f"[{self.dev_uuid}] Unable to send UNAVAILABLE status to OpenFactory: {e}")
        try:
            if self.sub:
                await self.sub.delete()
        except Exception:
            pass
        _active_device_defs.pop(self.dev_uuid, None)
        self.log.info(f"[{self.dev_uuid}] OPC UA connector removed succesfully")


async def monitor_device(device: Device, app: FastAPI):
    """
    Run subscription loop for a single device.

    This task manages the full monitoring lifecycle of a device, including:
      - Establishing the OPC UA session
      - Subscribing to variables and events
      - Forwarding updates to the global producer or queue
      - Handling reconnects and clean shutdowns

    Args:
        device (Device): Device schema instance defining OPC UA connector settings.
        app (FastAPI): The FastAPI application instance used to access shared state
    """
    monitor = DeviceMonitor(device, app)
    await monitor.run()
