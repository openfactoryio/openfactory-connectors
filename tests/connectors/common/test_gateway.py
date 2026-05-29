import unittest
from unittest.mock import Mock, patch

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

    def connect_device(self, device):
        return None

    def disconnect_device(self, device_uuid):
        return None


class IncompleteGateway(BaseGateway):
    """ Gateway missing required hook overrides for contract tests. """
    CONNECTOR_NAME = "TEST"


class MissingNameGateway(BaseGateway):
    """ Gateway missing CONNECTOR_NAME for contract tests. """
    pass


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


if __name__ == "__main__":
    unittest.main()
