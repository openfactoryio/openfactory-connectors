
import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from asyncua import ua

from connectors.opcua.gateway.src.monitor import DeviceMonitor


class OfflineVariableUpdateTests(unittest.IsolatedAsyncioTestCase):
    """Test safe handling of OPC UA variable updates."""

    def setUp(self):
        """Create a monitor and node without physical hardware."""
        self.monitor = object.__new__(DeviceMonitor)
        self.monitor.dev_uuid = "TEST-OPTA"
        self.monitor.log = Mock()
        self.monitor.connection_lost = False
        self.monitor._connection_generation = 0

        self.node = Mock()
        self.node.nodeid.to_string.return_value = "i=50078"
        self.node.read_value = AsyncMock(return_value=False)
        self.node.read_data_type_as_variant_type = AsyncMock(
            return_value=ua.VariantType.Boolean
        )
        self.node.write_value = AsyncMock()

    async def apply_update(self, value="true", generation=0):
        """Execute the proposed monitor update handler."""
        await self.monitor._apply_variable_update(
            node=self.node,
            name="relay_2",
            value=value,
            expected_generation=generation,
        )

    async def test_does_not_write_when_connection_lost(self):
        """An offline update must not touch the OPC UA node."""
        self.monitor.connection_lost = True

        await self.apply_update()

        self.node.read_value.assert_not_awaited()
        self.node.write_value.assert_not_awaited()

    async def test_handles_opcua_read_failure(self):
        """A failed read must not produce an unhandled task error."""
        self.node.read_value.side_effect = ConnectionError(
            "Timed out waiting for client to reconnect"
        )

        await self.apply_update()

        self.node.write_value.assert_not_awaited()
        self.monitor.log.warning.assert_called()

    async def test_discards_update_after_connection_generation_changes(self):
        """An update started before a disconnect must not write afterward."""
        read_started = asyncio.Event()
        finish_read = asyncio.Event()

        async def delayed_read():
            read_started.set()
            await finish_read.wait()
            return False

        self.node.read_value.side_effect = delayed_read

        task = asyncio.create_task(self.apply_update())

        await asyncio.wait_for(read_started.wait(), timeout=2)

        # Simulate a connection loss followed by recovery.
        self.monitor._connection_generation += 1
        self.monitor.connection_lost = False

        finish_read.set()
        await asyncio.wait_for(task, timeout=2)

        self.node.write_value.assert_not_awaited()

    async def test_writes_boolean_during_normal_connection(self):
        """A connected gateway should forward a typed Boolean update."""
        await self.apply_update(value="true")

        self.node.write_value.assert_awaited_once_with(
            True,
            varianttype=ua.VariantType.Boolean,
        )


if __name__ == "__main__":
    unittest.main()
