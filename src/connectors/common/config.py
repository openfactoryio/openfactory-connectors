import os

# Kafka producer
KAFKA_LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "5"))
