import pandas as pd
from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time

# Function to insert data into MongoDB
def insert_into_mongo(collection, message):
    collection.insert_one(message)

# Function to send data to Kafka
def send_to_kafka(producer, topic, message):
    producer.send(topic, value=message)
    print(f"Sent to Kafka: {message}")

def main():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Read customer service logs data from CSV
    csv_file_path = '/mnt/data/customer_service_logs.csv'  # Path to the uploaded file
    customer_data = pd.read_csv('/Users/navishatandon/Desktop/SDA-2/customer_service_logs.csv')
    
    # Initialize MongoDB client
    client = MongoClient("mongodb+srv://064091:PoW8dfs4HQvtlzC3@navisha1.6dbns.mongodb.net/")  # Replace with your MongoDB connection string
    db = client['customer_service_logs']  # Replace with your database name
    collection = db['interactions']  # Replace with your collection name

    print("Streaming data to Kafka and MongoDB...")
    for index, row in customer_data.iterrows():
        # Prepare the message
        message = {
            'interaction_type': row['interaction_type'],
            'customer_id': row['customer_id'],
            'agent_id': row['agent_id'],
            'call_duration_seconds': row['call_duration_seconds'],
            'is_failed': row['is_failed'],
            'is_ok': row['is_ok']
        }
        print(f"Processing record: {message}")

        # Send the message to Kafka
        send_to_kafka(producer, 'consumerdata', message)
        
        # Insert the message into MongoDB
        insert_into_mongo(collection, message)
        
        # Simulate real-time streaming
        time.sleep(1)

if __name__ == "__main__":
    main()
