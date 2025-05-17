
import requests
import json
import threading
import sys
import random
import time
from datetime import datetime
from kafka import KafkaConsumer

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
EMOJIS = ["🏏", "🎉", "👏", "🔥", "😊", "😢"]

# Function to send emoji data to the server automatically


def send_data(api_url, user_id):
    try:
        while True:
            # Generate a batch of emoji data
            emoji_data = generate_emoji_data(user_id)
            for emoji_entry in emoji_data:
                try:
                    response = requests.post(
                        f"{api_url}/submit_emoji", json=emoji_entry)
                    if response.status_code == 200:
                        pass
                        # print("Data sent successfully:", emoji_entry)
                    else:
                        print("Failed to send data:", response.json())
                except Exception as e:
                    print(f"Error sending data: {e}")
            time.sleep(2)  # Wait before sending the next batch
    except KeyboardInterrupt:
        print("\nStopping data sender...")

# Function to generate a batch of emoji data


def generate_emoji_data(user_id):
    emoji_count = random.randint(100, 200)  # Random count between 100 and 200
    data = []
    for _ in range(emoji_count):
        emoji = random.choice(EMOJIS)
        timestamp = datetime.now().isoformat()
        data.append({
            "user_id": user_id,
            "emoji_type": emoji,
            "timestamp": timestamp,
        })
    return data

# Function to disconnect a user from the topic


def disconnect_user(api_url, user_id):
    try:
        response = requests.post(f"{api_url}/disconnect/{user_id}")
        if response.status_code == 200:
            print(f"User {user_id} disconnected successfully.")
        else:
            print(f"Error disconnecting user {user_id}: {response.text}")
    except Exception as e:
        print(f"Error while disconnecting user {user_id}: {e}")

# Function to fetch or assign the subscriber topic for the user


def get_or_assign_subscriber_topic(api_url, user_id):
    try:
        # Attempt to fetch the subscriber topic for the user
        response = requests.get(f"{api_url}/get_user_topic/{user_id}")
        if response.status_code == 200:
            data = response.json()
            topic = data.get("subscriber_topic")
            if topic:
                print(f"User {user_id} assigned to topic: {topic}")
                return topic
            else:
                print("Error: No topic found in the response.")
        elif response.status_code == 400:
            print(response.json().get("error"))
        else:
            print(
                f"Error fetching topic: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error while fetching or assigning topic: {e}")
    return None

# Function to receive data from the user's subscriber topic


def receive_data(subscriber_topic):
    try:
        consumer = KafkaConsumer(
            subscriber_topic,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Listening to subscriber topic: {subscriber_topic}")
        for message in consumer:
            print(message.value)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    except Exception as e:
        print(f"Error while consuming data: {e}")


# Main function to run the send and receive functions in separate threads
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python client.py <port> <user_id>")
        sys.exit(1)

    port = sys.argv[1]
    user_id = sys.argv[2]
    api_url = f"http://localhost:{port}"  # API base URL

    try:
        # Fetch or assign the subscriber topic for the user
        subscriber_topic = get_or_assign_subscriber_topic(api_url, user_id)
        if not subscriber_topic:
            print(
                f"Error: Unable to assign a subscriber topic to user {user_id}. Exiting.")
            sys.exit(1)

        # Start threads for sending and receiving data
        sender_thread = threading.Thread(
            target=send_data, args=(api_url, user_id), daemon=True)
        receiver_thread = threading.Thread(
            target=receive_data, args=(subscriber_topic,), daemon=True)

        sender_thread.start()
        receiver_thread.start()

        # Wait for user interruption (KeyboardInterrupt)
        sender_thread.join()
        receiver_thread.join()

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Disconnecting user...")
        disconnect_user(api_url, user_id)
        print("Client exited cleanly.")
    except Exception as e:
        print(f"Unexpected error: {e}")
