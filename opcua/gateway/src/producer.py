"""
Global Kafka producer module for the OPC UA Gateway.

This module defines `GlobalAssetProducer`, a singleton Kafka producer
used to send asset attributes from all monitored OPC UA devices to
the configured `ASSETS_STREAM` Kafka topic.

The producer ensures:
- Only one instance exists globally.
- Messages are serialized to JSON.
- Integration with KSQLDB for topic resolution.
"""

import os
import json
from confluent_kafka import Producer
from openfactory.assets import AssetAttribute
from openfactory.kafka import KSQLDBClient


class GlobalAssetProducer(Producer):
    """
    Singleton Kafka producer for sending asset attributes from all OPC UA devices.

    This producer sends messages to the `ASSETS_STREAM` topic. It ensures
    only one instance exists globally.

    Attributes:
        ksql (KSQLDBClient): Client used to resolve Kafka topics.
        topic (str): Kafka topic name for sending asset attributes.
    """
    _instance = None

    def __new__(cls, *args, **kwargs) -> "GlobalAssetProducer":
        """ Ensure only one instance of the producer exists. """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, ksqlClient: KSQLDBClient, bootstrap_servers: str = None) -> None:
        """
        Initialize the Kafka producer singleton.

        Args:
            ksqlClient (KSQLDBClient): Client to fetch Kafka topic.
            bootstrap_servers (str, optional): Kafka bootstrap servers. Defaults to KAFKA_BROKER env var.
        """
        if hasattr(self, "_initialized"):
            return
        bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BROKER")
        super().__init__(
            {
                'bootstrap.servers': bootstrap_servers,
                'linger.ms': int(os.getenv("KAFKA_LINGER_MS", "5")),
            }
        )
        self.ksql = ksqlClient
        self.topic = self.ksql.get_kafka_topic("ASSETS_STREAM")
        self._initialized = True

    def send(self, asset_uuid: str, asset_attribute: AssetAttribute) -> None:
        """
        Send a Kafka message for the given asset and attribute.

        Args:
            asset_uuid (str): UUID of the asset.
            asset_attribute (AssetAttribute): Attribute data to send.

        Returns:
            None
        """
        msg = {
            "ID": asset_attribute.id,
            "VALUE": asset_attribute.value,
            "TAG": asset_attribute.tag,
            "TYPE": asset_attribute.type,
            "attributes": {"timestamp": asset_attribute.timestamp}
        }
        self.produce(topic=self.topic, key=asset_uuid, value=json.dumps(msg))
