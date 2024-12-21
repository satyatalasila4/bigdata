import signal
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Kafka Configuration
KAFKA_TOPIC = "sales_topic"
KAFKA_SERVER = "localhost:9092"
CONSUMER_GROUP = "sales_consumer_group_real_time"  # Use a unique consumer group for real-time processing

# MongoDB Configuration
MONGO_URI = "mongodb+srv://talasi53:399844@sales-db.ickz6.mongodb.net/sales_data?retryWrites=true&w=majority"
MONGO_DATABASE = "sales_data"
MONGO_COLLECTION = "sales_records"

# Batch Configuration
BATCH_SIZE = 10000  # Number of records to process in a batch

# To handle graceful shutdown
is_running = True

def signal_handler(sig, frame):
    global is_running
    print("Shutting down consumer...")
    is_running = False

def test_mongo_connection():
    """Test connection to MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command("ping")
        print("Successfully connected to MongoDB!")
        client.close()
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        raise

def clear_existing_data():
    """Clear all existing data from MongoDB collection."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        collection.delete_many({})  # Delete all documents in the collection
        print("Successfully cleared existing data from MongoDB.")
    except Exception as e:
        print(f"Error clearing existing data: {e}")
        raise

def consume_real_time_data_from_kafka():
    """Consume real-time data from Kafka and insert valid batches into MongoDB."""
    global is_running
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',  # Ensure only new messages are consumed
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP
    )

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    print("Waiting for real-time data from Kafka...")

    batch = []  # To accumulate records
    total_inserted = 0

    for message in consumer:
        if not is_running:
            break
        try:
            print(f"Consumed offset: {message.offset}, Partition: {message.partition}")  # Log offset and partition
            data = message.value
            print(f"Received message: {data}")  # Debug log to print received message

            # Check if the data is a valid dictionary or list of dictionaries
            if isinstance(data, dict):
                batch.append(data)
            elif isinstance(data, list):
                batch.extend(data)
            else:
                print("Invalid data format. Skipping.")
                continue

            # Insert batch when it reaches the specified size
            if len(batch) >= BATCH_SIZE:
                print(f"Inserting batch: {batch[:5]}...")  # Log first few records for verification
                collection.insert_many(batch)
                total_inserted += len(batch)
                print(f"Inserted {len(batch)} records into MongoDB. Total inserted: {total_inserted}.")
                batch = []
        except Exception as e:
            print(f"Error processing message or inserting to MongoDB: {e}")

    # Insert any remaining records when consumer stops
    if batch:
        try:
            print(f"Inserting final batch: {batch[:5]}...")  # Log first few records for verification
            collection.insert_many(batch)
            total_inserted += len(batch)
            print(f"Inserted {len(batch)} remaining records into MongoDB. Total inserted: {total_inserted}.")
        except Exception as e:
            print(f"Error inserting final batch: {e}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
    test_mongo_connection()
    clear_existing_data()
    consume_real_time_data_from_kafka()
    print("Consumer process exited gracefully.")
