import unittest
from connectors.common.coordinator import BaseCoordinator
from connectors.shdr.coordinator.src.main import SHDRCoordinator


class SHDRCoordinatorConfigurationTests(unittest.TestCase):
    """
    Unittests for SHDRCoordinator configuration.
    """

    def test_is_a_base_gateway(self):
        """ Test SHDRCoordinator inherits from BaseGateway. """
        self.assertTrue(issubclass(SHDRCoordinator, BaseCoordinator))

    def test_connector_name(self):
        """ Test SHDRGateway connector name. """
        self.assertEqual(SHDRCoordinator.CONNECTOR_NAME, "SHDR")


if __name__ == "__main__":
    unittest.main()
