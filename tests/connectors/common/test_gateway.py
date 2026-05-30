import unittest
from unittest.mock import Mock, patch
from openfactory.exceptions import OFAException
from connectors.common.gateway import BaseGateway


class FakeKSQLClient:
    """ Minimal ksqlDB client double used by BaseGateway tests. """

    def __init__(self):
        self.queries = []
        self.table_names = {"TEST_DEVICE_ASSIGNMENT_SOURCE", "TEST_DEVICE_ASSIGNMENT"}

    def query(self, query):
        self.queries.append(query)
        if "TEST.Coordinator" in query:
            return [{"ASSET_UUID": "TEST-COORDINATOR"}]
        return []

    def tables(self):
        return list(self.table_names)


class FakeCoordinatorAsset:
    """ Asset double that records gateway registration calls. """

    def __init__(self, asset_uuid, ksqlClient):
        self.asset_uuid = asset_uuid
        self.ksql = ksqlClient
        self.registered_gateways = []

    def wait_until(self, attribute_id, value, timeout=30, use_ksqlDB=False):
        return True

    def register_gateway(self, sender_uuid, gateway_uuid):
        self.registered_gateways.append((sender_uuid, gateway_uuid))


class ExampleGateway(BaseGateway):
    """ Concrete gateway used to exercise BaseGateway behavior. """

    CONNECTOR_NAME = "TEST"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connected_devices = []
        self.disconnected_devices = []

    def connect_device(self, device):
        self.connected_devices.append(device)

    def disconnect_device(self, device_uuid):
        self.disconnected_devices.append(device_uuid)


class IncompleteGateway(BaseGateway):
    """ Gateway missing required hook overrides for contract tests. """
    CONNECTOR_NAME = "TEST"


class MissingNameGateway(BaseGateway):
    """ Gateway missing CONNECTOR_NAME for contract tests. """
    pass


# Minimal valid OpenFactory Device configuration used throughout this test suite.
#
# IMPORTANT:
# If test_valid_device_fixture() starts failing, the OpenFactory Device or
# Connector schema has likely changed. In that case, update this fixture
# first before investigating failures in the coordinator tests below.
#
# Most registration tests depend on this fixture reaching the gateway
# registration logic. An invalid fixture will cause those tests to fail
# early during Device validation, producing misleading failures.
VALID_DEVICE_JSON = """
{
    "uuid": "DEVICE1",
    "connector": {
        "type": "shdr",
        "host": "127.0.0.1",
        "port": 7878
    }
}
"""


class BaseGatewayTests(unittest.TestCase):
    """
    Unittests for BaseGateway.
    """

    def test_requires_connector_name(self):
        """ Test that CONNECTOR_NAME must be defined. """
        with self.assertRaises(NotImplementedError):
            MissingNameGateway(ksqlClient=FakeKSQLClient(), test_mode=True)

    def test_assignment_names_are_derived_from_connector_name(self):
        """ Test that assignment resource names are derived from CONNECTOR_NAME. """
        gateway = object.__new__(ExampleGateway)

        self.assertEqual(gateway.assignment_source_table, "TEST_DEVICE_ASSIGNMENT_SOURCE")
        self.assertEqual(gateway.assignment_table, "TEST_DEVICE_ASSIGNMENT")
        self.assertEqual(gateway.assignment_topic, "test_device_assignment_topic")

    def test_base_device_hooks_must_be_overridden(self):
        """ Test that base device lifecycle hooks require subclass overrides. """
        gateway = object.__new__(IncompleteGateway)

        with self.assertRaises(NotImplementedError):
            BaseGateway.connect_device(gateway, device=None)
        with self.assertRaises(NotImplementedError):
            BaseGateway.disconnect_device(gateway, device_uuid="device-1")

    def test_initializes_in_test_mode_with_fake_coordinator_asset(self):
        """ Test gateway initialization in OpenFactory test mode. """
        ksql = FakeKSQLClient()

        with patch("connectors.common.gateway.Asset", FakeCoordinatorAsset):
            gateway = ExampleGateway(ksqlClient=ksql, test_mode=True)

        self.assertEqual(gateway.COORDINATOR_UUID, "TEST-COORDINATOR")
        self.assertIsInstance(gateway.coordinator, FakeCoordinatorAsset)
        self.assertEqual(gateway.coordinator.registered_gateways, [(gateway.asset_uuid, gateway.asset_uuid)])

    def test_wait_for_existence_uses_centralized_names(self):
        """ Test that table readiness checks use centralized assignment names. """
        gateway = object.__new__(ExampleGateway)
        object.__setattr__(gateway, "ksql", FakeKSQLClient())
        object.__setattr__(gateway, "logger", Mock())

        gateway.wait_for_existence_of_tables()

        self.assertEqual(set(gateway.ksql.tables()), {"TEST_DEVICE_ASSIGNMENT", "TEST_DEVICE_ASSIGNMENT_SOURCE"})

    def test_fetch_assigned_devices_uses_assignment_table(self):
        """ Test assigned device query uses centralized table name. """
        gateway = object.__new__(ExampleGateway)
        object.__setattr__(gateway, "ksql", FakeKSQLClient())
        gateway._fetch_assigned_devices()

        self.assertIn(
            gateway.assignment_table,
            gateway.ksql.queries[-1]
        )

    def test_fetch_device_configs_empty_list_returns_empty(self):
        """ Test empty device list returns no config query. """
        gateway = object.__new__(ExampleGateway)
        object.__setattr__(gateway, "ksql", FakeKSQLClient())

        self.assertEqual(gateway._fetch_device_configs([]), [])
        self.assertEqual(len(gateway.ksql.queries), 0)

    @patch("connectors.common.gateway.Asset", FakeCoordinatorAsset)
    def test_register_gateway_requires_valid_coordinator(self):
        """ Test invalid coordinator assets raise OFAException. """
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)

        def raise_type_error(*args, **kwargs):
            raise TypeError()

        gateway.coordinator.register_gateway = raise_type_error

        with self.assertRaises(OFAException):
            gateway.register_gateway()

    @patch("connectors.common.gateway.Asset", FakeCoordinatorAsset)
    def test_rebuild_gateway_state_with_no_devices_returns(self):
        """ Test rebuild exits when no devices are assigned. """
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)
        gateway._fetch_assigned_devices = lambda: []
        gateway.rebuild_gateway_state()

        self.assertEqual(len(gateway.connected_devices), 0)

    @patch("connectors.common.gateway.Asset", FakeCoordinatorAsset)
    def test_rebuild_gateway_state_skips_malformed_rows(self):
        """ Test malformed ksqlDB rows are ignored. """
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)
        gateway._fetch_assigned_devices = lambda: [{"DEVICE_UUID": "DEVICE1"}]
        gateway._fetch_device_configs = lambda uuids: [{"ASSET_UUID": "DEVICE1"}]

        with patch.object(gateway.logger, "warning") as warning:
            gateway.rebuild_gateway_state()

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertIn("Skipping malformed ksqlDB row", warning_messages[0])

    @patch("connectors.common.gateway.Asset", FakeCoordinatorAsset)
    def test_rebuild_gateway_state_handles_invalid_device_config(self):
        """ Test invalid device configs are handled gracefully. """
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)
        gateway._fetch_assigned_devices = lambda: [{"DEVICE_UUID": "DEVICE1"}]
        gateway._fetch_device_configs = lambda uuids: [
            {
                "ASSET_UUID": "DEVICE1",
                "CONNECTOR_CONFIG": '{"uuid":"DEVICE1"}'
            }
        ]

        with patch.object(gateway.logger, "warning") as warning:
            gateway.rebuild_gateway_state()

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertTrue(
            any(
                "Failed to connect device DEVICE1"
                in msg
                for msg in warning_messages
            )
        )

    @patch("connectors.common.gateway.Asset", FakeCoordinatorAsset)
    def test_rebuild_gateway_state_connects_assigned_devices(self):
        """ Test rebuild reconnects assigned devices. """
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)
        gateway._fetch_assigned_devices = lambda: [{"DEVICE_UUID": "DEVICE1"}]
        gateway._fetch_device_configs = lambda uuids: [
            {
                "ASSET_UUID": "DEVICE1",
                "CONNECTOR_CONFIG": VALID_DEVICE_JSON
            }
        ]
        gateway.rebuild_gateway_state()

        self.assertEqual(len(gateway.connected_devices), 1)
        self.assertEqual(
            gateway.connected_devices[0].uuid,
            "DEVICE1"
        )

    @patch("connectors.common.gateway.Asset", FakeCoordinatorAsset)
    def test_register_device_invalid_json_is_ignored(self):
        """ Test invalid device JSON is ignored. """
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)

        with patch.object(gateway.logger, "warning") as warning:
            gateway.register_device("not-json")

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertTrue(
            any(
                "Failed to connect device"
                in msg
                for msg in warning_messages
            )
        )

    @patch("connectors.common.gateway.Asset")
    def test_register_device_connects_device(self, asset_cls):
        """ Test register_device connects validated devices. """
        asset_cls.return_value = Mock()
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)
        gateway.register_device(VALID_DEVICE_JSON)

        self.assertEqual(len(gateway.connected_devices), 1)
        self.assertEqual(
            gateway.connected_devices[0].uuid,
            "DEVICE1"
        )

    @patch("connectors.common.gateway.Asset")
    def test_deregister_device_disconnects_device(self, asset_cls):
        """ Test deregistration disconnects the device. """
        asset_cls.return_value = Mock()
        gateway = ExampleGateway(ksqlClient=FakeKSQLClient(), test_mode=True)
        gateway.deregister_device("DEVICE1")

        self.assertEqual(
            gateway.disconnected_devices,
            ["DEVICE1"]
        )


if __name__ == "__main__":
    unittest.main()
