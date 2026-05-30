import unittest
from unittest.mock import Mock, patch
from connectors.shdr.gateway.src.main import SHDRGateway


class FakeConnector:
    """ Minimal SHDR connector double. """

    def __init__(self, connector_type="shdr"):
        self.type = connector_type
        self.host = "127.0.0.1"
        self.port = 7878
        self.data = {}


class FakeDevice:
    """ Minimal OpenFactory device double. """

    def __init__(self, uuid="DEVICE1", connector_type="shdr"):
        self.uuid = uuid
        self.connector = FakeConnector(connector_type)


class SHDRGatewayDeviceTests(unittest.TestCase):
    """
    Unittests for SHDR device registration and deregistration.
    """

    def setUp(self):
        """ Create a lightweight gateway instance. """
        self.gateway = object.__new__(SHDRGateway)

        object.__setattr__(self.gateway, "device_tasks", {})
        object.__setattr__(self.gateway, "devices", {})
        object.__setattr__(self.gateway, "logger", Mock())

    @patch("asyncio.create_task")
    def test_connect_device_registers_task(self, create_task):
        """ Test valid SHDR devices are registered. """
        task = Mock()
        create_task.return_value = task
        device = FakeDevice()

        self.gateway.connect_device(device)

        self.assertIn(device.uuid, self.gateway.devices)
        self.assertIn(device.uuid, self.gateway.device_tasks)
        self.assertIs(self.gateway.devices[device.uuid], device)
        self.assertIs(self.gateway.device_tasks[device.uuid], task)
        create_task.assert_called_once()

    @patch("asyncio.create_task")
    def test_connect_device_rejects_non_shdr_devices(self, create_task):
        """ Test non-SHDR devices are ignored. """
        device = FakeDevice(connector_type="opcua")

        with patch.object(self.gateway.logger, "warning") as warning:
            self.gateway.connect_device(device)

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertTrue(
            any(
                "not an SHDR device"
                in msg
                for msg in warning_messages
            )
        )

        self.assertEqual(len(self.gateway.devices), 0)
        self.assertEqual(len(self.gateway.device_tasks), 0)
        create_task.assert_not_called()

    @patch("asyncio.create_task")
    def test_connect_device_rejects_duplicate_devices(self, create_task):
        """ Test duplicate device registrations are ignored. """
        task = Mock()
        create_task.return_value = task
        device = FakeDevice()

        self.gateway.connect_device(device)

        with patch.object(self.gateway.logger, "warning") as warning:
            self.gateway.connect_device(device)

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertTrue(
            any(
                "already connected"
                in msg
                for msg in warning_messages
            )
        )

        create_task.assert_called_once()

    def test_disconnect_device_without_task_logs_warning(self):
        """ Test disconnecting unknown devices logs warning. """
        with patch.object(self.gateway.logger, "warning") as warning:
            self.gateway.disconnect_device("DEVICE1")

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertTrue(
            any(
                "had no active task associated"
                in msg
                for msg in warning_messages
            )
        )

    def test_disconnect_device_cancels_task(self):
        """ Test disconnecting a device cancels its task. """
        device = FakeDevice()
        task = Mock()
        self.gateway.devices[device.uuid] = device
        self.gateway.device_tasks[device.uuid] = task

        self.gateway.disconnect_device(device.uuid)

        task.cancel.assert_called_once()
        self.assertNotIn(device.uuid, self.gateway.devices)
        self.assertNotIn(device.uuid, self.gateway.device_tasks)

    @patch("asyncio.create_task")
    def test_connect_device_keeps_existing_task_on_duplicate_registration(self, create_task):
        """ Test duplicate registration does not replace active tasks. """
        original_task = Mock()
        create_task.return_value = original_task

        device = FakeDevice()

        self.gateway.connect_device(device)
        self.gateway.connect_device(device)

        self.assertIs(self.gateway.device_tasks[device.uuid], original_task)
        create_task.assert_called_once()

    def test_disconnect_device_removes_device_state(self):
        """ Test deregistration removes stored device state. """
        device = FakeDevice()
        task = Mock()
        self.gateway.devices[device.uuid] = device
        self.gateway.device_tasks[device.uuid] = task

        self.gateway.disconnect_device(device.uuid)

        self.assertEqual(self.gateway.devices, {})
        self.assertEqual(self.gateway.device_tasks, {})


if __name__ == "__main__":
    unittest.main()
