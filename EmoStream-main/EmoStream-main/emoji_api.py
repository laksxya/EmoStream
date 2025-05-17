
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from datetime import datetime
import json
import threading

# Create Flask App instances
app_5000 = Flask(__name__)
app_5001 = Flask(__name__)
app_5002 = Flask(__name__)

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
EMOJI_DATA_TOPIC = "emoji-topic"

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500
)

# Subscriber topic mapping
subscriber_topic_map = {
    "subscriber-topic1": [],
    "subscriber-topic2": [],
    "subscriber-topic3": [],
    "subscriber-topic4": [],
    "subscriber-topic5": [],
    "subscriber-topic6": [],
}
user_topic_map = {}  # Maps each user to a subscriber topic
active_users = set()  # Tracks currently connected users


# Assign a user to a topic
def assign_user_to_topic(user_id):
    for topic, users in subscriber_topic_map.items():
        if len(users) < 2:  # Max 2 users per topic
            user_topic_map[user_id] = topic
            users.append(user_id)
            print(f"User {user_id} assigned to topic {topic}")  # Debug log
            return topic
    print(f"No available topics for user {user_id}.")  # Debug log
    return None


# Disconnect a user
def disconnect_user(user_id):
    if user_id in active_users:
        active_users.remove(user_id)
        topic = user_topic_map.pop(user_id, None)
        if topic:
            subscriber_topic_map[topic].remove(user_id)
        print(f"User {user_id} disconnected and removed from topic {topic}.")
        return True
    print(f"User {user_id} is not active.")
    return False


def create_routes(app):
    @app.route('/submit_emoji', methods=['POST'])
    def submit_emoji():
        try:
            data = request.get_json()
            required_fields = ['user_id', 'emoji_type']

            # Validate payload
            if not all(field in data for field in required_fields):
                return jsonify({"error": "Missing fields in the request"}), 400

            user_id = data['user_id']

            # Check if the user is already active
            if user_id not in active_users:
                active_users.add(user_id)  # Mark user as active

            # Ensure user is assigned to a topic
            if user_id not in user_topic_map:
                topic = assign_user_to_topic(user_id)
                if not topic:
                    return jsonify({"error": "No available topics for user"}), 400

            # Add timestamp
            data['timestamp'] = datetime.utcnow().isoformat()

            # Send data to the general emoji data topic
            producer.send(EMOJI_DATA_TOPIC, data)
            print(f"Sent data to Kafka: {data}")  # Debug log
            return jsonify({"status": "success"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/get_user_topic/<user_id>', methods=['GET'])
    def get_user_topic(user_id):
        try:
            # Check if the user is already active
            if user_id in active_users:
                return jsonify({"error": f"User {user_id} is already active."}), 400

            # Assign the user to a topic if not already assigned
            active_users.add(user_id)
            topic = assign_user_to_topic(user_id)
            if topic:
                return jsonify({"subscriber_topic": topic}), 200
            else:
                return jsonify({"error": "No available topics for user"}), 400

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/disconnect/<user_id>', methods=['POST'])
    def handle_disconnect(user_id):
        try:
            if disconnect_user(user_id):
                return jsonify({"status": f"User {user_id} disconnected."}), 200
            else:
                return jsonify({"error": f"User {user_id} is not active."}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/health', methods=['GET'])
    def health_check():
        return jsonify({"status": "running"}), 200


# Add routes to all apps
create_routes(app_5000)
create_routes(app_5001)
create_routes(app_5002)


# Run the Flask Apps on different ports in separate threads
def run_app(app, port):
    app.run(host='0.0.0.0', port=port, threaded=True)


if __name__ == "__main__":
    threading.Thread(target=run_app, args=(
        app_5000, 5000), daemon=True).start()
    threading.Thread(target=run_app, args=(
        app_5001, 5001), daemon=True).start()
    threading.Thread(target=run_app, args=(
        app_5002, 5002), daemon=True).start()

    print("Emoji API is running on ports 5000, 5001, and 5002.")
    # Keep the main thread alive
    while True:
        pass
