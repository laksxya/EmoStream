
from kafka import KafkaConsumer, KafkaProducer
import json
import sys

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = sys.argv[1]
OUTPUT_TOPIC = sys.argv[2]


consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("in: ", INPUT_TOPIC)
for message in consumer:
	print(f'received {message.value}')
	producer.send(OUTPUT_TOPIC,message.value)
