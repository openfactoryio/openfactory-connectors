"""
Device monitoring module for the OPC UA Gateway.

This module defines the `DeviceMonitor` class, which manages the lifecycle
of OPC UA devices. Each device monitor handles:

- Parsing the OPC UA connector schema.
- Establishing and maintaining OPC UA client sessions.
- Discovering OPC UA methods and exposing them as OpenFactory commands.
- Reading configured constants.
- Subscribing to variables and events.
- Forwarding asset attributes to Kafka via `GlobalAssetProducer`.
- Automatic reconnection on errors.
- Cleanup when monitoring tasks are cancelled.
"""

from __future__ import annotations
import asyncio
import re
import unicodedata
import json
from typing import Any, TYPE_CHECKING
from numbers import Number
from asyncua import Client, Node, ua
from asyncua.common.ua_utils import string_to_val
from asyncua.common.subscription import DataChangeEvent, StatusChangeEvent, OpcEvent, Subscription
from openfactory.schemas.devices import Device
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema
from openfactory.schemas.command_header import CommandEnvelope
from openfactory.assets import Asset, AssetAttribute
from openfactory.assets.utils import openfactory_timestamp, current_timestamp
from .utils import get_node_by_path, normalize_value, opcua_data_timestamp, opcua_event_timestamp

if TYPE_CHECKING:
    from .main import OPCUAGateway


RECONNECT_DELAY = 5.0


class DeviceMonitor:
    """
    Manage the lifecycle of an OPC UA monitored device.

    Each device gets its own monitor instance, which handles:
    - Parsing connector schema
    - Establishing OPC UA sessions
    - Discovering OPC UA methods and exposing OpenFactory commands
    - Reading configured constants
    - Subscribing to variables and events
    - Updating asset attributes in Kafka/ksqlDB
    - Handling reconnects and cleanup
    """

    def __init__(self, device: Device, app: OPCUAGateway):
        """
        Initialize monitoring of a given device.

        Args:
            device (Device): The device definition, including UUID and connector schema.
            app (OPCUAGateway): The OPCUAGateway application instance used to access shared state
        """
        self.device = device
        self.app = app
        self.dev_uuid = device.uuid
        self.asset = Asset(device.uuid, ksqlClient=app.ksql)
        self.global_producer = app.global_producer
        self.schema = OPCUAConnectorSchema(**device.connector.model_dump())
        self.log = app.logger
        self.methods: dict[str, dict] = {}
        self.node_map: dict = {}
        self.client: Client | None = None
        self.connection_lost = False

    async def run(self) -> None:
        """
        Main entry point for the device monitor.

        Continuously attempts to connect to the OPC UA server. Once connected,
        the session is managed by `_run_session()`. If the initial connection
        fails, the monitor retries after a short delay. If the session ends,
        a new connection attempt is started.
        """
        self.log.info(f"Starting monitor task for {self.dev_uuid}")

        try:
            while True:
                client = Client(self.schema.server.uri)

                async def on_connection_lost(exc: Exception) -> None:
                    self.connection_lost = True
                    self.log.warning(
                        f"[{self.dev_uuid}] Lost connection to OPC UA server ({exc}). "
                        f"Will automatically reconnect when the server becomes available."
                    )
                    await self._publish_availability(False)

                client.connection_lost_callback = on_connection_lost

                try:
                    await client.connect(auto_reconnect=True, reconnect_max_delay=2.0)
                    self.connection_lost = False
                    await self._run_session(client)

                except asyncio.CancelledError:
                    raise

                except Exception as e:
                    self.log.warning(f"[{self.dev_uuid}] Unable to connect to OPC UA server: {e}")
                    await self._publish_availability(False)
                    await asyncio.sleep(RECONNECT_DELAY)

                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        self.log.exception(f"[{self.dev_uuid}] Error while disconnecting OPC UA client")

        finally:
            try:
                await self._cleanup()
            except Exception:
                self.log.exception(f"[{self.dev_uuid}] Error while cleaning up")

    async def _publish_availability(self, available: bool) -> None:
        """
        Publish the availability state of the monitored OPC UA device.

        Args:
            available (bool): True if the device is available, False otherwise.
        """
        try:
            self.global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=AssetAttribute(
                    id="avail",
                    value="AVAILABLE" if available else "UNAVAILABLE",
                    tag="Availability",
                    type="Events",
                ),
            )
        except Exception:
            self.log.exception(f"[{self.dev_uuid}] Failed to publish {'AVAILABLE' if available else 'UNAVAILABLE'}")

    async def _process_data_change(self, ev: DataChangeEvent) -> None:
        """
        Process a data change notification received from an OPC UA subscription.

        Args:
            ev (DataChangeEvent): Data change notification yielded by the subscription iterator.
        """
        ingestion_timestamp = current_timestamp()

        info = self.node_map.get(ev.node.nodeid)
        if info is None:
            self.log.warning(f"Received data change for unknown node {ev.node.nodeid}")
            return

        local_name = info["local_name"]
        tag = info["tag"]
        deadband = info["deadband"]

        # Normalize value
        val = normalize_value(ev.value)

        # Determine OpenFactory type
        ofa_type = "Samples" if isinstance(val, Number) else "Events"
        if isinstance(val, bool):
            ofa_type = "Events"

        # Check status
        data_value: ua.DataValue = ev.data.monitored_item.Value

        if not data_value.StatusCode.is_good():
            self.log.warning(
                f"Received bad / uncertain value for {local_name} ({tag}): "
                f"StatusCode={data_value.StatusCode}"
            )
            val = "UNAVAILABLE"

        # Apply deadband
        if ofa_type == "Samples" and val != "UNAVAILABLE":
            last_val = info["last_val"]

            if last_val is not None and abs(val - float(last_val)) <= deadband:
                return

            info["last_val"] = val

        self.log.debug(f"[{self.dev_uuid}] Data change {local_name} ({tag}): {val}")

        device_timestamp = opcua_data_timestamp(data_value)

        attribute = AssetAttribute(
            id=local_name,
            value=val,
            type=ofa_type,
            tag=tag,
            timestamp=openfactory_timestamp(device_timestamp),
        )

        try:
            self.global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=attribute,
                ingestion_timestamp=ingestion_timestamp,
            )

        except Exception:
            self.log.exception(f"Failed to send data to Kafka for {self.dev_uuid}")

    async def _process_event(self, ev: OpcEvent) -> None:
        """
        Process an event notification received from an OPC UA subscription.

        Args:
            ev (OpcEvent): Event notification yielded by the subscription iterator.
        """
        ingestion_timestamp = current_timestamp()

        event = ev.event

        try:
            message = getattr(event, "Message", None)
            severity = getattr(event, "Severity", None)
            source_name = getattr(event, "SourceName", "opcua")

            # Resolve event type name
            if event.EventType is not None:
                event_type_node = self.client.get_node(event.EventType)
                browse_name = await event_type_node.read_browse_name()
                tag = browse_name.Name
            else:
                tag = "OPCUAEvent"

            if message and hasattr(message, "Text"):
                message_text = message.Text
            else:
                message_text = str(message)

            if severity:
                message_text = f"{message_text} (Severity: {severity})"

            self.log.debug(f"[{self.dev_uuid}] Event from {source_name}: {message_text}")

            device_timestamp = opcua_event_timestamp(event)

            attribute = AssetAttribute(
                id=f"{source_name}_event",
                value=message_text,
                type="Condition",
                tag=tag,
                timestamp=openfactory_timestamp(device_timestamp),
            )

            self.global_producer.send(
                asset_uuid=self.dev_uuid,
                asset_attribute=attribute,
                ingestion_timestamp=ingestion_timestamp,
            )

        except Exception:
            self.log.exception(f"[{self.dev_uuid}] Failed to process OPC UA event")

    async def _run_session(self, client: Client) -> None:
        """
        Establish a session, discover configured resources,
        and subscribe to variables and events.

        Runs until the subscription terminates or the monitor is cancelled.
        """
        self.log.info(f"Connected to OPC UA server {self.schema.server.uri}")
        self.client = client

        async with await client.create_subscription(float(self.schema.server.subscription.publishing_interval)) as sub:

            # Discover methods
            await self._discover_methods(client)

            # Read constants
            await self._read_constants(client)

            # Subscribe to variables
            self.node_map = await self._subscribe_variables(client, sub)

            # Subscribe to events
            await self._subscribe_events(client, sub)

            # OPC UA server is ready
            try:
                await self._publish_availability(True)
            except Exception:
                self.log.exception(f"[{self.dev_uuid}] Failed to publish AVAILABLE")

            # Process subscription notifications
            async for event in sub:

                if self.connection_lost:
                    self.connection_lost = False
                    self.log.info(f"[{self.dev_uuid}] Reconnected to OPC UA server {self.schema.server.uri}")
                    await self._publish_availability(True)

                match event:

                    case DataChangeEvent() as ev:
                        await self._process_data_change(ev)

                    case OpcEvent() as ev:
                        await self._process_event(ev)

                    case StatusChangeEvent() as ev:
                        self.log.info(f"Subscription status changed: {ev.notification.Status}")

                        if ev.notification.Status.is_bad():
                            break

    async def call_opcua_method(self, cmd: str, envelope: CommandEnvelope) -> Any | None:
        """
        Invoke an OPC UA method using an OpenFactory command envelope.

        Args:
            cmd (str): Command name
            envelope (CommandEnvelope): Command envelope containing header and named arguments

        Returns:
            Any | None: Result from the OPC UA method call, or None if the call fails.
        """
        # Get method metadata from cache
        parent_node = self.methods[cmd]["parent_object"]
        method_node = self.methods[cmd]["method_node"]
        input_vtypes = self.methods[cmd]["input_vtypes"]
        name_to_index = self.methods[cmd]["name_to_index"]

        # Extract arguments dict from envelope
        args_dict = envelope.arguments or {}

        # Prepare positional argument list for OPC UA
        ua_args: list[ua.Variant] = [None] * len(input_vtypes)

        for name, val_str in args_dict.items():
            if name not in name_to_index:
                self.log.warning(f"[{self.dev_uuid}] Unknown argument '{name}' for command '{cmd}'")
                continue
            idx = name_to_index[name]
            vtype = input_vtypes[idx]
            ua_args[idx] = ua.Variant(string_to_val(val_str, vtype), vtype)

        # Fill any missing arguments with empty strings
        for i, arg in enumerate(ua_args):
            if arg is None:
                ua_args[i] = ua.Variant("", input_vtypes[i])

        try:
            self.log.debug(f"[{self.dev_uuid}] Calling OPC UA method '{cmd}' with arguments {ua_args}")
            result = await parent_node.call_method(method_node, *ua_args)
            self.log.debug(f"[{self.dev_uuid}] Method '{cmd}' result: {result}")
            return result

        except Exception:
            self.log.exception(f"[{self.dev_uuid}] Method call failed for '{cmd}'")

    def _normalize_cmd(self, name: str) -> str:
        """
        Normalize a command name into a safe, ASCII-only identifier.

        This function transforms the input string by:
          1. Converting Unicode characters to closest ASCII equivalents (e.g., 'ä' → 'a').
          2. Replacing any non-alphanumeric characters with underscores.
          3. Collapsing multiple consecutive underscores into a single underscore.
          4. Stripping leading and trailing underscores.

        Args:
            name (str): The original command name to normalize.

        Returns:
            str: A normalized, ASCII-only string suitable for use as a safe internal identifier.

        Examples:
            >>> self._normalize_cmd("Generate Code")
            'Generate_Code'
            >>> self._normalize_cmd("Öl Pumpe Start!")
            'Ol_Pumpe_Start'
            >>> self._normalize_cmd("Temp °C")
            'Temp_C'
        """
        # Normalize unicode (ä → a, etc.)
        name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()

        # Replace non-alphanumeric with underscore
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)

        # Collapse multiple underscores
        name = re.sub(r"_+", "_", name)

        return name.strip("_")

    async def _discover_methods(self, client: Client) -> None:
        """
        Discover configured OPC UA methods and expose them as
        OpenFactory commands.

        Args:
            client (Client): Connected OPC UA client.
        """
        if not self.schema.methods:
            return

        for local_name, method_cfg in self.schema.methods.items():

            self.log.debug(f"[{self.dev_uuid}] Add Methods for {local_name}")
            if method_cfg.browse_path is not None:
                methods_node = await get_node_by_path(client, method_cfg.browse_path)
            else:
                methods_node = await get_node_by_path(client, method_cfg.node_id)

            for method_node in await methods_node.get_methods():
                method = await method_node.read_browse_name()
                command = self._normalize_cmd(method.Name)
                description = await method_node.read_description()
                self.log.info(f"[{self.dev_uuid}] Add Method: {command} ({description.Text})")

                # Build structured method contract that will be added to OpenFactory Asset
                method_contract = {
                    "description": description.Text if description else "",
                    "arguments": []
                }

                vtypes = []
                name_to_index = {}
                try:
                    # Get the InputArguments property
                    input_args_node = await method_node.get_child("0:InputArguments")
                    input_args_values = await input_args_node.read_value()

                    # Discover variant type of input arguments
                    for pos, spec in enumerate(input_args_values):
                        vtype = ua.VariantType(spec.DataType.Identifier)
                        vtypes.append(vtype)
                        name_to_index[spec.Name] = pos

                        # Log structured information
                        desc = spec.Description.Text if spec.Description else "No description"
                        self.log.info(
                            f"[{self.dev_uuid}] [{command}] Input argument: "
                            f"Name='{spec.Name}' "
                            f"Type={vtype.name} "
                            f"Description='{desc}'"
                        )

                        method_contract["arguments"].append({
                            "name": spec.Name,
                            "description": desc
                        })

                except ua.UaStatusCodeError as e:
                    if e.code == ua.StatusCodes.BadNoMatch:
                        # No InputArguments found; ua_args remains an empty list
                        input_args_values = []
                    else:
                        raise

                # store reference to OPC UA method (map between OpenFactory command and OPC UA method)
                self.methods[command] = {
                    "parent_object": methods_node,
                    "method_node": method_node,
                    "input_vtypes": vtypes,
                    "name_to_index": name_to_index,
                }

                # add method to Asset
                self.asset.add_attribute(
                    asset_attribute=AssetAttribute(
                        id=command,
                        value=json.dumps(method_contract),
                        type='Method',
                        tag='Method'
                    )
                )

                # add OpenFactory command to Asset
                self.asset.add_attribute(
                    asset_attribute=AssetAttribute(
                        id=f"{command}_CMD",
                        value="",
                        type='OpenFactory',
                        tag='Method.Command'
                    )
                )

                # subscribe to OpenFactory command directed to the Asset
                def on_cmd(msg_key, msg_value, cmd=command):
                    try:
                        # Parse JSON string into CommandEnvelope
                        envelope = CommandEnvelope.parse_raw(msg_value['VALUE'])
                    except Exception as e:
                        self.log.error(f"[{self.dev_uuid}] Failed to parse CommandEnvelope for '{cmd}': {e}")
                        return

                    # Schedule the OPC UA method call with typed envelope
                    asyncio.create_task(self.call_opcua_method(cmd=cmd, envelope=envelope))

                self.asset.subscribe_to_attribute(f'{command}_CMD', on_cmd)

    async def _read_constants(self, client: Client) -> None:
        """
        Read all configured constants and read all configured constants
        and add them to the OpenFactory asset.

        Args:
            client (Client): Connected OPC UA client
        """
        if not self.schema.constants:
            return
        for local_name, const_cfg in self.schema.constants.items():
            try:
                if const_cfg.browse_path is not None:
                    const_node = await get_node_by_path(client, const_cfg.browse_path)
                else:
                    const_node = client.get_node(const_cfg.node_id)

                dv = await const_node.read_data_value()
                val = normalize_value(dv.Value.Value)
                ofa_type = "Samples" if isinstance(val, Number) else "Events"

                # Booleans are treated as Events, not Samples
                if isinstance(val, bool):
                    ofa_type = "Events"

                attr = AssetAttribute(
                    id=local_name,
                    value=val,
                    type=ofa_type,
                    tag=const_cfg.tag,
                    timestamp=openfactory_timestamp(dv.SourceTimestamp)
                    )
                self.asset.add_attribute(asset_attribute=attr)
                self.log.info(f"[{self.dev_uuid}] Acquired constant '{local_name}' ({const_node.nodeid.to_string()}): {dv.Value.Value}")
            except ValueError as e:
                self.log.error(f"[{self.dev_uuid}] Failed to to read '{local_name}' ({const_node.nodeid.to_string()}):{e}")
            except Exception:
                self.log.exception(f"[{self.dev_uuid}] Failed to read '{local_name}' ({const_node.nodeid.to_string()})")

    async def _subscribe_variables(self, client: Client, sub) -> dict[ua.NodeId, dict[str, Any]]:
        """
        Subscribe to all configured variables.

        Args:
            client (Client): Connected OPC UA client.
            sub (Subscription): Active OPC UA subscription.

        Returns:
            dict: Mapping from OPC UA NodeId to variable metadata.
        """
        node_map = {}

        if not self.schema.variables:
            return node_map

        for local_name, var_cfg in self.schema.variables.items():
            try:
                if var_cfg.browse_path is not None:
                    var_node = await get_node_by_path(client, var_cfg.browse_path)
                else:
                    var_node = client.get_node(var_cfg.node_id)

                await sub.subscribe_data_change(
                    var_node,
                    queuesize=var_cfg.queue_size,
                    sampling_interval=var_cfg.sampling_interval,
                )

                node_map[var_node.nodeid] = {
                    "local_name": local_name,
                    "tag": var_cfg.tag,
                    "deadband": var_cfg.deadband,
                    "last_val": None
                }

                self.log.info(f"[{self.dev_uuid}] Subscribed to variable '{local_name}' ({var_node.nodeid.to_string()})")

                # Discover server-side access level
                user_access_level = await var_node.get_user_access_level()
                is_writable = ua.AccessLevel.CurrentWrite in user_access_level

                # Policy (schema) vs capability (server)
                if var_cfg.access_level == "rw":
                    if is_writable:
                        loop = asyncio.get_running_loop()

                        async def _write_value(value: str, node: Node, name: str) -> None:
                            """ Write value to OPC UA server. """
                            try:
                                v_type = await node.read_data_type_as_variant_type()
                                await node.write_value(value, varianttype=v_type)
                            except Exception:
                                self.log.exception(f"[{self.dev_uuid}] Failed to write variable '{name}' ({node.nodeid.to_string()})")

                        def on_variable_update(msg_subject: str, msg_value: dict, node: Node = var_node, name: str = local_name) -> None:
                            """ Callback for attribute changes, capturing loop variables. """

                            async def _check_and_write(value: str):
                                """ Avoid infinity loops. """
                                current_val = await node.read_value()
                                if value != current_val:
                                    self.log.debug(f"[{self.dev_uuid}] Updating variable '{name}' ({node.nodeid.to_string()}) with '{value}'")
                                    await _write_value(value, node=node, name=name)

                            loop.create_task(_check_and_write(msg_value["VALUE"]))

                        # Schedule write-back to OPC UA server if OpenFactory detects a change
                        self.asset.subscribe_to_attribute(local_name, on_variable_update)

                        self.log.info(
                            f"[{self.dev_uuid}] Allow OpenFactory to write to "
                            f"variable '{local_name}' ({var_node.nodeid.to_string()})"
                        )
                    else:
                        self.log.warning(
                            f"[{self.dev_uuid}] Variable '{local_name}' ({var_node.nodeid.to_string()}) "
                            f"declared as rw but server reports it as read-only"
                        )

            except ValueError as e:
                self.log.error(f"[{self.dev_uuid}] Failed to subscribe to variable '{local_name}' ({var_node.nodeid.to_string()}):{e}")
            except Exception:
                self.log.exception(f"[{self.dev_uuid}] Failed to subscribe to variable '{local_name}' ({var_node.nodeid.to_string()})")

        return node_map

    async def _subscribe_events(self, client: Client, sub: Subscription) -> None:
        """
        Subscribe to all configured OPC UA event sources.

        Args:
            client (Client): Connected OPC UA client.
            sub (Subscription): Active OPC UA subscription.
        """
        if not self.schema.events:
            return

        for local_name, event_cfg in self.schema.events.items():
            try:
                if event_cfg.browse_path is not None:
                    event_node = await get_node_by_path(client, event_cfg.browse_path)
                else:
                    event_node = client.get_node(event_cfg.node_id)

                await sub.subscribe_events(event_node)

                self.log.info(
                    f"[{self.dev_uuid}] Subscribed to events "
                    f"'{local_name}' ({event_node.nodeid.to_string()})"
                )

            except Exception:
                self.log.exception(f"[{self.dev_uuid}] Failed to subscribe to events '{local_name}'")

    async def _cleanup(self) -> None:
        """ Release resources associated with the monitored device. """
        self.log.info(f"[{self.dev_uuid}] Monitor task cancelled; cleaning up")
        try:
            if self.subscription:
                await self.subscription.delete()
        except Exception:
            pass

        try:
            self.asset.close()
        except Exception:
            pass
        self.log.info(f"[{self.dev_uuid}] OPC UA connector removed successfully")
