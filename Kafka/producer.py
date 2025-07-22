import csv
from kafka import KafkaProducer
import json
import time
import os
import uuid

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'Nifty_index_data_topic' # Topic name remains as planned
CSV_FILE = 'Nifty_index_data.csv' # Assuming you've renamed your local file to this

def serialize_data(row):
    """
    Serializes dictionary data from CSV into JSON bytes.
    Adds a unique ID and performs type conversions for specific columns
    relevant to the NIFTY Index Data dataset.
    """
    processed_row = {}
    # Add a unique ID for each record
    processed_row['id'] = str(uuid.uuid4())

    for key, value in row.items():
        # Handle empty strings or None values uniformly
        if value is None or str(value).strip() == '':
            processed_row[key] = None
        # Numerical Columns (using exact column names from your CSV header)
        # Assuming 'Shares Traded' is integer, others are float
        elif key in ["Open", "High", "Low", "Close", "Turnover"]:
            try:
                processed_row[key] = float(value) # Convert to float for prices and turnover
            except (ValueError, TypeError):
                processed_row[key] = None # Set to None if conversion fails
        elif key == "Shares Traded":
            try:
                processed_row[key] = int(float(value)) # Convert to int for shares traded
            except (ValueError, TypeError):
                processed_row[key] = None # Set to None if conversion fails
        # All other columns (like 'Date') - keep as strings (strip whitespace)
        else:
            processed_row[key] = str(value).strip()

    return json.dumps(processed_row).encode('utf-8')

def produce_messages():
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=serialize_data
        )
        print(f"Producer connected to KAFKA_BROKER: {KAFKA_BROKER}")

        with open(CSV_FILE, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            print(f"Reading data from '{CSV_FILE}' and sending to topic '{TOPIC_NAME}'...")
            message_count = 0
            for row in csv_reader:
                producer.send(TOPIC_NAME, row)
                # Adjusted logging message to use relevant Nifty index data fields
                # Using original row values for logging before serialization modifies them
                print(f"Sent message (Date: {row.get('Date', 'N/A')}, Open: {row.get('Open', 'N/A')}, Close: {row.get('Close', 'N/A')}, Shares Traded: {row.get('Shares Traded', 'N/A')}, Turnover: {row.get('Turnover', 'N/A')})")
                message_count += 1
                time.sleep(0.05) # Small delay to simulate real-time data flow (reduced slightly)

            producer.flush()
            print(f"\nSuccessfully sent {message_count} messages to Kafka.")

    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE}' was not found. Please ensure it's in the same directory as the script or the path is correct.")
    except Exception as e:
        print(f"Error producing messages: {e}")
        print("Please ensure your Kafka container is running and accessible at the specified broker address.")
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if producer:
            producer.close()
            print("Producer connection closed.")

if __name__ == "__main__":
    produce_messages()
