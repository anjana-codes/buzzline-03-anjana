"""
json_producer_anjana.py

Continuously stream JSON messages about Nepal to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

import json
import time
from kafka import KafkaProducer


#####################################
# Kafka Configuration
#####################################

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'anjana_json_topic'

#####################################
# Kafka Producer
#####################################

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#####################################
# Messages to Send
#####################################

messages = [
    {"message": "Nepal is home to Mount Everest, the highest peak in the world.", "author": "Anjana"},
    {"message": "Kathmandu Valley is rich with temples, stupas, and cultural heritage.", "author": "Anjana"},
    {"message": "The flag of Nepal is the only non-rectangular national flag in the world.", "author": "Anjana"},
    {"message": "Nepal has diverse geography, from the Himalayas to the Terai plains.", "author": "Anjana"},
]

#####################################
# Send Messages in a Loop
#####################################

print(f"Streaming messages to topic '{KAFKA_TOPIC}' continuously...")

try:
    while True:
        for msg in messages:
            print(f"Sending: {msg}")
            producer.send(KAFKA_TOPIC, value=msg)
            time.sleep(1)  # Wait 1 second between messages

except KeyboardInterrupt:
    print("Producer interrupted by user.")

except Exception as e:
    print(f"Error: {e}")

finally:
    producer.close()
    print("Producer closed.")
