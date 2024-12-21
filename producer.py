import os
import csv
import json
from kafka import KafkaProducer
from kaggle.api.kaggle_api_extended import KaggleApi

# Configuration
DATASET = "okhiriadaveoseghale/100000-sales-records"
FILE_NAME = "100000 Sales Records.csv"
KAGGLE_PATH = "./new_kaggle_data"  # New folder for dataset
KAFKA_TOPIC = "sales_topic"
KAFKA_SERVER = "localhost:9092"
BATCH_SIZE = 100  # Number of rows per batch

# Function to download dataset from Kaggle
def download_kaggle_dataset():
    """Downloads and extracts the dataset from Kaggle into a new folder."""
    try:
        os.makedirs(KAGGLE_PATH, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(DATASET, path=KAGGLE_PATH, unzip=True)
        print(f"Dataset downloaded and extracted to {KAGGLE_PATH}")
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        raise

# Function to stream data to Kafka in batches
def stream_data_to_kafka():
    """Streams data to a Kafka topic in batches."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        file_path = os.path.join(KAGGLE_PATH, FILE_NAME)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        batch = []
        total_batches = 0
        total_rows = 0

        with open(file_path, mode='r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                batch.append(row)
                total_rows += 1

                if len(batch) >= BATCH_SIZE:
                    producer.send(KAFKA_TOPIC, batch)
                    total_batches += 1
                    batch = []

            if batch:  # Send any remaining rows in the last batch
                producer.send(KAFKA_TOPIC, batch)
                total_batches += 1

        producer.close()
        print(f"Data streaming completed: {total_rows} rows sent in {total_batches} batches.")

    except Exception as e:
        print(f"Error streaming data to Kafka: {e}")
        raise

# Main
if __name__ == "__main__":
    try:
        print("Starting dataset download...")
        download_kaggle_dataset()

        print("Starting data streaming to Kafka...")
        stream_data_to_kafka()

    except Exception as e:
        print(f"Error in main execution: {e}")
