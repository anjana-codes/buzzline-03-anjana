"""
csv_producer_anjana.py

continuous stream CSV-like data (timestamp + temperature) to a Kafka topic using pandas.
"""

#####################################
# Import Modules
#####################################

import json
import time
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timedelta

#####################################
# Kafka Configuration
#####################################

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'anjana_csv_topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#####################################
# Generate CSV-like Data
#####################################

# Start timestamp
start_time = datetime(2025, 1, 1, 15, 0, 0)

# Sample temperatures
temperatures = [70.4, 70.8, 71.2, 71.5]

print(f"Streaming CSV messages to topic '{KAFKA_TOPIC}' continuously...")

try:
    while True:
        for i, temp in enumerate(temperatures):
            timestamp = (start_time + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
            msg = {"timestamp": timestamp, "temperature": temp}
            print(f"Sending: {msg}")
            producer.send(KAFKA_TOPIC, value=msg)
            time.sleep(1)

except KeyboardInterrupt:
    print("Producer interrupted by user.")

finally:
    producer.close()
    print("Producer closed.")
