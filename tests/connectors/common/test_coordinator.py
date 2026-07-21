import json
import unittest
import os
from unittest.mock import patch
from openfactory.schemas.devices import Device
from connectors.common.coordinator import BaseCoordinator


class FakeKSQLClient:
    """ Minimal ksqlDB client double used by BaseCoordinator tests. """
    def __init__(self):
        self.queries = []
        self.statements = []
        self.inserts = []

    def query(self, query):
        self.queries.append(query)
        return []

    def statement_query(self, statement):
        self.statements.append(statement)

    def insert_into_stream(self, stream, rows):
        self.inserts.append((stream, rows))


class ExampleCoordinator(BaseCoordinator):
    """ Concrete coordinator used to exercise BaseCoordinator behavior. """
    CONNECTOR_NAME = "TEST"
    asset_uuid = "COORDINATOR"
    registered_metrics = False

    def assign_gateway(self):
        return "TEST-GATEWAY"

    def register_prometheus_metrics(self, metrics_port, metrics_path):
        self.registered_metrics = True


class MissingNameCoordinator(BaseCoordinator):
    """ Coordinator missing CONNECTOR_NAME for contract tests. """
    pass


class FakeAvailability:
    def __init__(self, value):
        self.value = value


class FakeAsset:
    """ Minimal Asset double used by registration tests. """

    def __init__(self, asset_uuid, ksqlClient=None):
        self.asset_uuid = asset_uuid
        self.avail = FakeAvailability("AVAILABLE")
        self.register_calls = []
        self.deregister_calls = []
        self.closed = False

        self.raise_register_type_error = False
        self.raise_deregister_type_error = False

    def register_device(self, **kwargs):
        if self.raise_register_type_error:
            raise TypeError()

        self.register_calls.append(kwargs)

    def deregister_device(self, **kwargs):
        if self.raise_deregister_type_error:
            raise TypeError()

        self.deregister_calls.append(kwargs)

    def close(self):
        self.closed = True


class FakeProducer:
    """ Minimal Kafka producer double. """

    def __init__(self):
        self.messages = []

    def produce(self, **kwargs):
        self.messages.append(kwargs)


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


class BaseCoordinatorTests(unittest.TestCase):
    """
    Unittests for BaseCoordinator
    """

    @patch.dict(os.environ, {"LOG_HTTP_REQUESTS": "true"})
    def test_log_http_requests_enabled(self):
        """ Test LOG_HTTP_REQUESTS enables HTTP request logging. """
        coordinator = ExampleCoordinator(
            ksqlClient=FakeKSQLClient(),
            test_mode=True,
        )

        self.assertTrue(coordinator.log_http_requests)

    @patch.dict(os.environ, {}, clear=True)
    def test_log_http_requests_disabled_by_default(self):
        """ Test HTTP request logging is disabled by default. """
        coordinator = ExampleCoordinator(
            ksqlClient=FakeKSQLClient(),
            test_mode=True,
        )

        self.assertFalse(coordinator.log_http_requests)

    def test_valid_device_fixture(self):
        """
        Test that VALID_DEVICE_JSON remains a valid OpenFactory Device.

        If this test fails, the OpenFactory Device or Connector schema has
        likely changed and the fixture VALID_DEVICE_JSON must be updated. Fix this test
        before investigating failures in other coordinator registration tests.
        """
        Device(**json.loads(VALID_DEVICE_JSON))

    def test_requires_connector_name(self):
        """ Test that CONNECTOR_NAME must be defined. """
        with self.assertRaises(NotImplementedError):
            MissingNameCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)

    def test_assignment_names_are_derived_from_connector_name(self):
        """ Test that assignment resource names are derived from CONNECTOR_NAME. """
        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)

        self.assertEqual(coordinator.assignment_source_table, "TEST_DEVICE_ASSIGNMENT_SOURCE")
        self.assertEqual(coordinator.assignment_table, "TEST_DEVICE_ASSIGNMENT")
        self.assertEqual(coordinator.assignment_topic, "test_device_assignment_topic")

    def test_assignment_tables_use_centralized_names(self):
        """ Test that created ksqlDB statements use the centralized name helpers. """
        ksql = FakeKSQLClient()
        coordinator = ExampleCoordinator(ksqlClient=ksql, test_mode=True)

        self.assertEqual(len(ksql.statements), 2)
        self.assertIn(coordinator.assignment_source_table, ksql.statements[0])
        self.assertIn(coordinator.assignment_topic, ksql.statements[0])
        self.assertIn(coordinator.assignment_table, ksql.statements[1])
        self.assertIn(coordinator.assignment_source_table, ksql.statements[1])

    def test_create_device_assignment_tables_creates_two_statements(self):
        """ Test that create_device_assignment_tables issues two statements. """
        ksql = FakeKSQLClient()
        ExampleCoordinator(ksqlClient=ksql, test_mode=True)

        self.assertEqual(len(ksql.statements), 2)
        self.assertIn("CREATE TABLE IF NOT EXISTS", ksql.statements[0])
        self.assertIn("CREATE TABLE IF NOT EXISTS", ksql.statements[1])

    def test_discover_gateways_registers_all_found_gateways(self):
        """ Test all gateways discovered are registered. """
        ksql = FakeKSQLClient()

        def fake_query(query):
            return [
                {"ASSET_UUID": "GW1"},
                {"ASSET_UUID": "GW2"},
            ]

        ksql.query = fake_query
        coordinator = ExampleCoordinator(ksqlClient=ksql, test_mode=True)

        self.assertIn("GW1", coordinator.gateways)
        self.assertIn("GW2", coordinator.gateways)

    def test_register_prometheus_metrics_is_called(self):
        """ Test coordinator register Prometheus metrics. """
        ksql = FakeKSQLClient()
        coordinator = ExampleCoordinator(ksqlClient=ksql, test_mode=True)
        self.assertTrue(coordinator.registered_metrics)

    def test_get_assigned_gateway_queries_assignment_table(self):
        """ Test that assignment query uses correct table. """
        ksql = FakeKSQLClient()
        coordinator = ExampleCoordinator(ksqlClient=ksql, test_mode=True)
        coordinator.get_assigned_gateway_uuid("DEVICE1")

        self.assertIn(coordinator.assignment_table, ksql.queries[-1])

    def test_get_assigned_gateway_uuid_returns_none_when_not_found(self):
        """ Test get_assigned_gateway_uuid returns None if no gateway is assigned. """
        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)
        self.assertIsNone(coordinator.get_assigned_gateway_uuid("DEVICE1"))

    def test_get_assigned_gateway_uuid_returns_gateway(self):
        """ Test get_assigned_gateway_uuid returns the assigned gateway. """
        ksql = FakeKSQLClient()
        coordinator = ExampleCoordinator(ksqlClient=ksql, test_mode=True)

        def fake_query(query):
            return [{"GATEWAY_UUID": "GW1"}]

        ksql.query = fake_query
        self.assertEqual(coordinator.get_assigned_gateway_uuid("DEVICE1"), "GW1")

    def test_discover_gateways_uses_connector_gateway_type(self):
        """ Test discover_gateways uses correct name for Gateway Type. """
        ksql = FakeKSQLClient()
        ExampleCoordinator(ksqlClient=ksql, test_mode=True)

        self.assertTrue(
            any(
                "TEST.Gateway" in query
                for query in ksql.queries
            )
        )

    def test_register_gateway_avoids_duplicates(self):
        """ Test that register_gateway does not register duplicates """
        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)
        coordinator.gateways.clear()
        coordinator.register_gateway("GW1")
        coordinator.register_gateway("GW1")

        self.assertEqual(coordinator.gateways, ["GW1"])

    def test_register_device_invalid_json_is_ignored(self):
        """ Test invalid device JSON is ignored. """
        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)

        with patch.object(coordinator.logger, "warning") as warning:
            coordinator.register_device("not-json")

        self.assertIn("Failed to register device not-json", warning.call_args[0][0])

    @patch("connectors.common.coordinator.Asset")
    def test_register_device_records_assignment(self, asset_cls):
        """ Test successful device registration records assignment. """
        ksql = FakeKSQLClient()

        asset_cls.return_value = FakeAsset("TEST-GATEWAY")

        coordinator = ExampleCoordinator(ksqlClient=ksql, test_mode=True)
        coordinator.register_device(VALID_DEVICE_JSON)

        self.assertEqual(len(ksql.inserts), 1)

        self.assertEqual(
            ksql.inserts[0][0],
            coordinator.assignment_source_table
        )

        self.assertEqual(
            ksql.inserts[0][1][0]["DEVICE_UUID"],
            "DEVICE1"
        )

        self.assertEqual(
            ksql.inserts[0][1][0]["GATEWAY_UUID"],
            "TEST-GATEWAY"
        )

    @patch("connectors.common.coordinator.Asset")
    def test_register_device_skips_unavailable_gateway(self, asset_cls):
        """ Test device registration fails if gateway is unavailable. """
        asset = FakeAsset("TEST-GATEWAY")
        asset.avail.value = "UNAVAILABLE"
        asset_cls.return_value = asset

        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)

        with patch.object(coordinator.logger, "warning") as warning:
            coordinator.register_device(VALID_DEVICE_JSON)

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertIn(
            "Gateway 'TEST-GATEWAY' is not AVAILABLE",
            warning_messages
        )

    @patch("connectors.common.coordinator.Asset")
    def test_register_device_handles_invalid_gateway_asset(self, asset_cls):
        """ Test invalid gateway assets are handled gracefully. """
        asset = FakeAsset("TEST-GATEWAY")
        asset.raise_register_type_error = True
        asset_cls.return_value = asset

        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)

        with patch.object(coordinator.logger, "warning") as warning:
            coordinator.register_device(VALID_DEVICE_JSON)

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertIn(
            f"Asset '{asset.asset_uuid}' does not appear to be a valid gateway.",
            warning_messages
        )

    def test_deregister_device_without_assignment_returns(self):
        """ Test deregistration aborts when no gateway is assigned. """
        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)
        coordinator.producer = FakeProducer()
        coordinator.get_assigned_gateway_uuid = lambda _: None

        with patch.object(coordinator.logger, "warning") as warning:
            coordinator.deregister_device("DEVICE1")

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertIn(
            "Aborting deregistration as no associated Gateway was found",
            warning_messages
        )

        # no message was sent
        self.assertEqual(len(coordinator.producer.messages), 0)

    @patch("connectors.common.coordinator.Asset")
    def test_deregister_device_produces_tombstone(self, asset_cls):
        """ Test successful deregistration produces assignment tombstone. """
        asset_cls.return_value = FakeAsset("GW1")

        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)
        coordinator.producer = FakeProducer()
        coordinator.get_assigned_gateway_uuid = lambda _: "GW1"
        coordinator.deregister_device("DEVICE1")

        self.assertEqual(len(coordinator.producer.messages), 1)

        self.assertEqual(
            coordinator.producer.messages[0]["topic"],
            coordinator.assignment_topic
        )

        self.assertEqual(
            coordinator.producer.messages[0]["key"],
            "DEVICE1"
        )

        self.assertIsNone(coordinator.producer.messages[0]["value"])

    @patch("connectors.common.coordinator.Asset")
    def test_deregister_device_skips_unavailable_gateway(self, asset_cls):
        """ Test deregistration fails if gateway is unavailable. """
        asset = FakeAsset("GW1")
        asset.avail.value = "UNAVAILABLE"
        asset_cls.return_value = asset

        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)
        coordinator.producer = FakeProducer()
        coordinator.get_assigned_gateway_uuid = lambda _: "GW1"

        with patch.object(coordinator.logger, "warning") as warning:
            coordinator.deregister_device("DEVICE1")

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertIn(
            "Gateway 'GW1' is not AVAILABLE",
            warning_messages
        )

        # no message was sent
        self.assertEqual(len(coordinator.producer.messages), 0)

    @patch("connectors.common.coordinator.Asset")
    def test_deregister_device_handles_invalid_gateway_asset(self, asset_cls):
        """ Test invalid gateway assets are handled gracefully. """
        asset = FakeAsset("GW1")
        asset.raise_deregister_type_error = True
        asset_cls.return_value = asset

        coordinator = ExampleCoordinator(ksqlClient=FakeKSQLClient(), test_mode=True)
        coordinator.producer = FakeProducer()
        coordinator.get_assigned_gateway_uuid = lambda _: "GW1"

        with patch.object(coordinator.logger, "warning") as warning:
            coordinator.deregister_device("DEVICE1")

        warning_messages = [
            call.args[0]
            for call in warning.call_args_list
        ]

        self.assertIn(
            "Asset 'GW1' does not appear to be a valid gateway.",
            warning_messages
        )


if __name__ == "__main__":
    unittest.main()
