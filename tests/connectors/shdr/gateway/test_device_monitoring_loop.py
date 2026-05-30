import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch
from connectors.shdr.gateway.src.main import SHDRGateway


class FakeDataPoint:
    """ Minimal SHDR datapoint double. """

    def __init__(self, tag="Temperature", type="Samples"):
        self.tag = tag
        self.type = type


class FakeConnector:
    """ Minimal SHDR connector double. """

    def __init__(self):
        self.type = "shdr"
        self.host = "127.0.0.1"
        self.port = 7878
        self.data = {
            "temp": FakeDataPoint()
        }


class FakeDevice:
    """ Minimal SHDR device double. """

    def __init__(self, uuid="DEVICE1"):
        self.uuid = uuid
        self.connector = FakeConnector()


class FakeWriter:
    """ Minimal asyncio writer double. """

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class SHDRGatewayDeviceLoopTests(unittest.IsolatedAsyncioTestCase):
    """
    Unittests for SHDR device monitoring loop.
    """

    def setUp(self):
        """ Create a lightweight gateway instance. """
        self.gateway = object.__new__(SHDRGateway)

        object.__setattr__(self.gateway, "logger", Mock())
        object.__setattr__(self.gateway, "global_producer", Mock())

    async def test_device_loop_marks_device_available_on_connect(self):
        """ Test successful connections mark devices available. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = asyncio.CancelledError()

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, FakeWriter()))):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        send = self.gateway.global_producer.send.call_args_list[0]

        self.assertEqual(send.kwargs["asset_uuid"], device.uuid)
        self.assertEqual(send.kwargs["asset_attribute"].id, "avail")
        self.assertEqual(send.kwargs["asset_attribute"].value, "AVAILABLE")

    async def test_device_loop_marks_device_unavailable_on_connection_error(self):
        """ Test connection errors mark devices unavailable. """
        device = FakeDevice()

        with patch("asyncio.open_connection", AsyncMock(side_effect=RuntimeError("boom"))):
            with patch("asyncio.sleep", AsyncMock(side_effect=asyncio.CancelledError())):
                with self.assertRaises(asyncio.CancelledError):
                    await self.gateway._device_loop(device)

        sends = self.gateway.global_producer.send.call_args_list

        self.assertTrue(
            any(
                call.kwargs["asset_attribute"].value == "UNAVAILABLE"
                for call in sends
            )
        )

    async def test_device_loop_ignores_unknown_shdr_keys(self):
        """ Test unknown SHDR keys are ignored. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = [
            b"2026-05-28T00:10:00Z|unknown|123\n",
            asyncio.CancelledError()
        ]

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, FakeWriter()))):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        warning_messages = [
            call.args[0]
            for call in self.gateway.logger.warning.call_args_list
        ]

        self.assertTrue(
            any(
                "Unknown SHDR key"
                in msg
                for msg in warning_messages
            )
        )

    async def test_device_loop_publishes_shdr_datapoints(self):
        """ Test SHDR datapoints are published. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = [
            b"2026-05-28T00:10:00Z|temp|42.1\n",
            asyncio.CancelledError()
        ]

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, FakeWriter()))):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        sends = self.gateway.global_producer.send.call_args_list

        datapoint_calls = [
            call
            for call in sends
            if call.kwargs["asset_attribute"].id == "temp"
        ]

        self.assertEqual(len(datapoint_calls), 1)
        datapoint = datapoint_calls[0].kwargs["asset_attribute"]
        self.assertEqual(datapoint.value, "42.1")
        self.assertEqual(datapoint.tag, "Temperature")
        self.assertEqual(datapoint.timestamp, "2026-05-28T00:10:00Z")

    async def test_device_loop_logs_closed_stream(self):
        """ Test closed SHDR streams are detected. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.return_value = b""

        open_connection = AsyncMock()
        open_connection.side_effect = [
            (reader, FakeWriter()),
            asyncio.CancelledError()
        ]

        with patch("asyncio.open_connection", open_connection):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        warning_messages = [
            call.args[0]
            for call in self.gateway.logger.warning.call_args_list
        ]

        self.assertTrue(
            any(
                "SHDR stream closed"
                in msg
                for msg in warning_messages
            )
        )

    async def test_device_loop_propagates_cancellation(self):
        """ Test task cancellation is propagated. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = asyncio.CancelledError()

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, FakeWriter()))):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        debug_messages = [
            call.args[0]
            for call in self.gateway.logger.debug.call_args_list
        ]

        self.assertTrue(
            any(
                "Stopping SHDR monitoring task"
                in msg
                for msg in debug_messages
            )
        )

    async def test_device_loop_closes_writer(self):
        """ Test TCP writer is closed on shutdown. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = asyncio.CancelledError()

        writer = FakeWriter()

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, writer))):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        self.assertTrue(writer.closed)

    async def test_device_loop_ignores_unknown_keys_and_continues(self):
        """ Test unknown SHDR keys do not block valid datapoints. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = [
            b"2026-05-28T00:10:00Z|unknown|123|temp|42.1\n",
            asyncio.CancelledError()
        ]

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, FakeWriter()))):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        warning_messages = [
            call.args[0]
            for call in self.gateway.logger.warning.call_args_list
        ]

        self.assertTrue(
            any(
                "Unknown SHDR key"
                in msg
                for msg in warning_messages
            )
        )

        sends = self.gateway.global_producer.send.call_args_list

        datapoint_calls = [
            call
            for call in sends
            if call.kwargs["asset_attribute"].id == "temp"
        ]

        self.assertEqual(len(datapoint_calls), 1)
        datapoint = datapoint_calls[0].kwargs["asset_attribute"]
        self.assertEqual(datapoint.value, "42.1")
        self.assertEqual(datapoint.tag, "Temperature")

    async def test_device_loop_marks_device_unavailable_when_stream_closes(self):
        """ Test closed streams mark devices unavailable. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.return_value = b""

        open_connection = AsyncMock()
        open_connection.side_effect = [
            (reader, FakeWriter()),
            asyncio.CancelledError()
        ]

        with patch("asyncio.open_connection", open_connection):
            with self.assertRaises(asyncio.CancelledError):
                await self.gateway._device_loop(device)

        sends = self.gateway.global_producer.send.call_args_list

        self.assertTrue(
            any(
                call.kwargs["asset_attribute"].id == "avail"
                and call.kwargs["asset_attribute"].value == "UNAVAILABLE"
                for call in sends
            )
        )

    async def test_device_loop_handles_invalid_shdr_lines(self):
        """ Test invalid SHDR lines mark devices unavailable. """
        device = FakeDevice()
        reader = AsyncMock()
        reader.readline.side_effect = [
            b"invalid\n"
        ]

        with patch("asyncio.open_connection", AsyncMock(return_value=(reader, FakeWriter()))):
            with patch("asyncio.sleep", AsyncMock(side_effect=asyncio.CancelledError())):
                with self.assertRaises(asyncio.CancelledError):
                    await self.gateway._device_loop(device)

        sends = self.gateway.global_producer.send.call_args_list

        self.assertTrue(
            any(
                call.kwargs["asset_attribute"].id == "avail"
                and call.kwargs["asset_attribute"].value == "UNAVAILABLE"
                for call in sends
            )
        )

        warning_messages = [
            call.args[0]
            for call in self.gateway.logger.warning.call_args_list
        ]

        self.assertTrue(
            any(
                device.uuid in msg
                and "disconnected" in msg
                for msg in warning_messages
            )
        )

    async def test_device_loop_retries_after_connection_failure(self):
        """ Test connection failures trigger reconnection attempts. """
        device = FakeDevice()
        open_connection = AsyncMock()
        open_connection.side_effect = [
            RuntimeError("boom"),
            asyncio.CancelledError()
        ]

        with patch("asyncio.open_connection", open_connection):
            with patch("asyncio.sleep", AsyncMock()):
                with self.assertRaises(asyncio.CancelledError):
                    await self.gateway._device_loop(device)

        self.assertEqual(open_connection.call_count, 2)

if __name__ == "__main__":
    unittest.main()
