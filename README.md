# EmoStream: Concurrent Emoji Broadcast over Event-Driven Architecture

## Overview
EmoStream captures and processes live emoji reactions during sports events. It uses Kafka and Spark Streaming to handle high traffic with low delay, giving users a real-time experience.


## How It Works
### 1. Receiving Emoji Data
- `emoji_api.py` handles client requests.
- Users send `user_id`, `emoji_type`, and `timestamp`.
- Kafka producer stores data, flushing every 500ms.

### 2. Processing Data
- `spark_streaming_job.py` consumes emoji data from Kafka.
- Micro-batches process data every 2 seconds.
- Aggregation ensures 1000 similar emojis count as 1.

### 3. Broadcasting Results
- `main_publisher.py` distributes aggregated data.
- `cluster_producer.py` and `subscriber.py` ensure real-time updates.
- Clients receive results in sync with their input.

## Setup
### Installation
1. Clone the repo:
   ```sh
   git clone https://github.com/yourusername/emostream.git
   cd emostream
   ```
2. Start Kafka:
   ```sh
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```
3. Run API server:
   ```sh
   python emoji_api.py
   ```
4. Start Spark job:
   ```sh
   spark-submit spark_streaming_job.py
   ```
5. Start publisher & subscribers:
   ```sh
   python main_publisher.py
   python cluster_producer.py
   python subscriber.py
   ```

## Usage
### Send Emoji Data
```sh
curl -X POST http://localhost:5000/send_emoji \
     -H "Content-Type: application/json" \
     -d '{"user_id": "123", "emoji_type": "🔥", "timestamp": "1700000000"}'
```
### Receive Processed Data
Subscribers get emoji stats every 2 seconds.

## Contributors
- Lakshya Vijay ([GitHub](https://github.com/laksxya))
- Naveen Nair ([GitHub](https://github.com/fl1x12))
- Shanmuga Teja ([GitHub](https://github.com/NaveenNair04))
- Maitreya Tiwary ([GitHub](https://github.com/MaitreyaTiwary))


