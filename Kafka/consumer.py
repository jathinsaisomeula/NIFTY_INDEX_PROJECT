from kafka import KafkaConsumer
import json
import os
import time

# --- Configuration ---
# IMPORTANT: This script runs LOCALLY on your macOS.
# Based on your 'docker compose ps' output, Kafka is accessible on kafka:9092.
# For LOCAL execution, 'kafka:9092' is usually the correct default.
# 'kafka:9092' would be used if this consumer itself was running inside a Docker container
# and connecting to the 'kafka' service within the same Docker network.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
print(f"Consumer attempting to connect to KAFKA_BROKER: {KAFKA_BROKER}") # Added here as requested
TOPIC_NAME = 'Nifty_index_data_topic' # Changed to match producer's topic name
GROUP_ID = 'NIFTY_CONSUMER_GROUP' # New group ID for NIFTY data

def deserialize_data(data_bytes):
    """Deserialize JSON bytes from Kafka message."""
    decoded_string = data_bytes.decode('utf-8')
    return json.loads(decoded_string)

def consume_messages():
    consumer = None
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=deserialize_data,
            # request_timeout_ms=30000, # Uncomment if you face connection timeouts
            # api_version=(0, 10, 1) # Uncomment if you encounter API version mismatch errors
        )
        # print(f"Consumer attempting to connect to KAFKA_BROKER: {KAFKA_BROKER}") # Removed duplicate from here
        print(f"Subscribed to topic: '{TOPIC_NAME}' in group '{GROUP_ID}'. Waiting for messages... (Press Ctrl+C to stop)")

        # Process messages
        for message in consumer:
            nifty_record = message.value # 'message.value' is now a Python dictionary
            print(f"--- Received NIFTY Record (Offset: {message.offset}, Partition: {message.partition}) ---")
            # Print specific fields for better readability of structured data
            print(f"  Date: {nifty_record.get('Date', 'N/A')}")
            print(f"  Open: {nifty_record.get('Open', 'N/A')}")
            print(f"  High: {nifty_record.get('High', 'N/A')}")
            print(f"  Low: {nifty_record.get('Low', 'N/A')}")
            print(f"  Close: {nifty_record.get('Close', 'N/A')}")
            print(f"  Shares Traded: {nifty_record.get('Shares Traded', 'N/A')}")
            print(f"  Turnover: {nifty_record.get('Turnover', 'N/A')}")
            print("-" * 40)
            time.sleep(0.05) # Small delay to simulate processing

    except KeyboardInterrupt:
        print("\nConsumer gracefully stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
        print("Please ensure your Kafka container is running, the topic exists, and the broker address is correct.")
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if consumer:
            consumer.close()
            print("Consumer connection closed.")

if __name__ == "__main__":
    consume_messages()