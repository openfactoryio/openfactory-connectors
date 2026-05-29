import unittest
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

    def assign_gateway(self):
        return "TEST-GATEWAY"


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

    def register_device(self, **kwargs):
        self.register_calls.append(kwargs)

    def deregister_device(self, **kwargs):
        self.deregister_calls.append(kwargs)

    def close(self):
        self.closed = True


class FakeProducer:
    """ Minimal Kafka producer double. """

    def __init__(self):
        self.messages = []

    def produce(self, **kwargs):
        self.messages.append(kwargs)


class BaseCoordinatorTests(unittest.TestCase):
    """
    Unittests for BaseCoordinator
    """

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
        coordinator.register_device("not-json")


if __name__ == "__main__":
    unittest.main()
