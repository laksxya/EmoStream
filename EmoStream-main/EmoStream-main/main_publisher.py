
from kafka import KafkaConsumer, KafkaProducer
import json

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
AGGREGATED_TOPIC = "aggregated-emoji-topic"
CLUSTER_TOPIC_1 = "cluster-topic-1"
CLUSTER_TOPIC_2 = "cluster-topic-2"
CLUSTER_TOPIC_3 = "cluster-topic-3"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    AGGREGATED_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Main Publisher is running...")

# Consume and forward messages
for message in consumer:
    # Extract original timestamp
    timestamp = message.value.get('timestamp', '')

    # Create concatenated emoji string
    emoji_concat = "".join(
        emoji['emoji_type'] * emoji['scaled_count']
        for emoji in message.value.get('emoji_data', [])
    )

    # Prepare new message with concatenated emojis
    forwarded_message = {
        #'timestamp': timestamp,
        'emoji_string': emoji_concat
    }

    print(f"Received aggregated data: {message.value}")
    #print(f"Forwarded emoji string: {emoji_concat}")

    # Forward to cluster publishers
    producer.send(CLUSTER_TOPIC_1, emoji_concat)
    producer.send(CLUSTER_TOPIC_2, emoji_concat)
    producer.send(CLUSTER_TOPIC_3, emoji_concat)
