import unittest
from connectors.common.gateway import BaseGateway
from connectors.shdr.gateway.src.main import SHDRGateway


class SHDRGatewayConfigurationTests(unittest.TestCase):
    """
    Unittests for SHDRGateway configuration.
    """

    def test_is_a_base_gateway(self):
        """ Test SHDRGateway inherits from BaseGateway. """
        self.assertTrue(issubclass(SHDRGateway, BaseGateway))

    def test_connector_name(self):
        """ Test SHDRGateway connector name. """
        self.assertEqual(SHDRGateway.CONNECTOR_NAME, "SHDR")


if __name__ == "__main__":
    unittest.main()
