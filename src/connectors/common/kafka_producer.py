"""
Global Kafka producer module.

This module defines `GlobalAssetProducer`, a singleton Kafka producer
used to send asset attributes from all monitored devices to
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
from .config import KAFKA_LINGER_MS
from . import gateway_metrics


def stats_callback(stats_json):

    # https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
    stats = json.loads(stats_json)

    gateway_metrics.KAFKA_TX_MESSAGES.set(stats.get("txmsgs", 0))
    gateway_metrics.KAFKA_TX_BYTES.set(stats.get("tx_bytes", 0))
    gateway_metrics.KAFKA_QUEUE_MESSAGES.set(stats["msg_cnt"])
    gateway_metrics.KAFKA_QUEUE_BYTES.set(stats["msg_size"])

    for broker_name, broker in stats.get("brokers", {}).items():
        rtt = broker.get("rtt", {})
        gateway_metrics.KAFKA_BROKER_RTT_AVG.labels(broker=broker_name).set(rtt.get("avg", 0) / 1_000_000)
        gateway_metrics.KAFKA_BROKER_RTT_P95.labels(broker=broker_name).set(rtt.get("p95", 0) / 1_000_000)
        gateway_metrics.KAFKA_BROKER_TX_ERRORS.labels(broker=broker_name).set(broker.get("txerrs", 0))


class GlobalAssetProducer(Producer):
    """
    Kafka producer for sending asset attributes from all OPC UA devices.

    This producer sends messages to the `ASSETS_STREAM` topic. It ensures
    only one instance exists globally.

    Attributes:
        ksql (KSQLDBClient): Client used to resolve Kafka topics.
        topic (str): Kafka topic name for sending asset attributes.
    """

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
                'linger.ms': KAFKA_LINGER_MS,
                'acks': 1,                                    # waits until the leader partition has received the message
                'retries': 0,                                 # don't retry on errors
                "statistics.interval.ms": 15000,              # statistics every 15s
            },
            stats_cb=stats_callback
        )
        self.ksql = ksqlClient
        self.topic = self.ksql.get_kafka_topic("ASSETS_STREAM")
        self._initialized = True

    def send(self, asset_uuid: str, asset_attribute: AssetAttribute, ingestion_timestamp: str | None = None) -> None:
        """
        Send a Kafka message for the given asset and attribute.

        Args:
            asset_uuid (str): UUID of the asset.
            asset_attribute (AssetAttribute): Attribute data to send.
            ingestion_timestamp (str, optional): Timestamp at ingestion time.

        Returns:
            None
        """
        if ingestion_timestamp:
            msg = {
                "ID": asset_attribute.id,
                "VALUE": asset_attribute.value,
                "TAG": asset_attribute.tag,
                "TYPE": asset_attribute.type,
                "attributes": {
                    "timestamp": asset_attribute.timestamp,
                    "ingestion_timestamp": ingestion_timestamp,
                    },
            }
        else:
            msg = {
                "ID": asset_attribute.id,
                "VALUE": asset_attribute.value,
                "TAG": asset_attribute.tag,
                "TYPE": asset_attribute.type,
                "attributes": {
                    "timestamp": asset_attribute.timestamp,
                    },
            }
        self.produce(topic=self.topic, key=asset_uuid, value=json.dumps(msg))
        self.poll(0)
