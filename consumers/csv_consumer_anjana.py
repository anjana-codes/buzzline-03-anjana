"""
csv_consumer_anjana.py

Continuously consumes CSV-like messages from Kafka and performs real-time analytics using pandas.
"""

#####################################
# Import Modules
#####################################

import json
import logging
from kafka import KafkaConsumer
import pandas as pd

#####################################
# Logger Setup
#####################################

logging.basicConfig(
    level=logging.DEBUG,
    filename="csv_consumer_anjana_loop.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

#####################################
# Kafka Configuration
#####################################

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'anjana_csv_topic'
GROUP_ID = 'csv-group-anjana-loop'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Listening for messages on topic '{KAFKA_TOPIC}' continuously...")

#####################################
# Consume Messages in Loop
#####################################

messages = []

try:
    for message in consumer:
        record = message.value
        logger.debug(f"Consumed CSV message: {record}")
        messages.append(record)

        # Real-time analytics example: alert if temperature > 71
        if record["temperature"] > 71:
            print(f"⚡ ALERT: High temperature -> {record['temperature']} at {record['timestamp']}")

        # Show DataFrame every 3 messages
        if len(messages) % 3 == 0:
            df = pd.DataFrame(messages)
            print("\nCurrent batch of messages:\n", df)

except KeyboardInterrupt:
    print("Consumer interrupted by user.")
    logger.warning("Consumer interrupted by user.")

except Exception as e:
    logger.error(f"Unexpected error in consumer: {e}")
    print(f"Error: {e}")

finally:
    consumer.close()
    print("Consumer closed.")
    logger.info("Kafka consumer closed.")
