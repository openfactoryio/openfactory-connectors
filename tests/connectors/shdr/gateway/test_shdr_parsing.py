import unittest

from connectors.shdr.gateway.src.main import SHDRGateway


class SHDRGatewayParserTests(unittest.TestCase):
    """
    Unittests for SHDRGateway line parsing
    """

    def setUp(self):
        """ Create a lightweight gateway instance without starting OpenFactory. """
        self.gateway = object.__new__(SHDRGateway)

    def test_parse_shdr_line(self):
        """ Test parsing a valid SHDR line into timestamp and values. """
        parsed = self.gateway.parse_shdr_line("2026-05-28T00:10:00Z|temp|42.1|humi|71")

        self.assertEqual(parsed["timestamp"], "2026-05-28T00:10:00Z")
        self.assertEqual(parsed["values"], {"temp": "42.1", "humi": "71"})

    def test_parse_shdr_line_uses_current_timestamp_when_empty(self):
        """ Test that an empty timestamp falls back to the current UTC timestamp. """
        parsed = self.gateway.parse_shdr_line("|temp|42.1")

        self.assertTrue(parsed["timestamp"].endswith("Z"))
        self.assertEqual(parsed["values"], {"temp": "42.1"})

    def test_parse_shdr_line_rejects_short_lines(self):
        """ Test that too-short SHDR lines are rejected. """
        with self.assertRaises(ValueError):
            self.gateway.parse_shdr_line("2026-05-28T00:10:00Z|temp")

    def test_parse_shdr_line_rejects_malformed_pairs(self):
        """ Test that malformed SHDR key/value pairs are rejected. """
        with self.assertRaises(ValueError):
            self.gateway.parse_shdr_line("2026-05-28T00:10:00Z|temp|42.1|humi")

    def test_parse_shdr_line_strips_surrounding_whitespace(self):
        """ Test parsing ignores surrounding whitespace. """
        parsed = self.gateway.parse_shdr_line("  2026-05-28T00:10:00Z| temp |42.1  ")

        self.assertEqual(parsed["timestamp"], "2026-05-28T00:10:00Z")
        self.assertEqual(parsed["values"], {"temp": "42.1"})

    def test_parse_shdr_line_parses_multiple_datapoints(self):
        """ Test parsing SHDR lines with multiple datapoints. """
        parsed = self.gateway.parse_shdr_line("2026-05-28T00:10:00Z|a|1|b|2|c|3")

        self.assertEqual(
            parsed["values"],
            {
                "a": "1",
                "b": "2",
                "c": "3"
            }
        )


if __name__ == "__main__":
    unittest.main()
