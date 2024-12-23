from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'consumerdata',  # Replace with your Kafka topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb+srv://064091:PoW8dfs4HQvtlzC3@navisha1.6dbns.mongodb.net/')
db = mongo_client['customer_service_logs']  # Database name
customer_service_collection = db['customer_service']  # Collection for customer service data
alerts_collection = db['alerts']  # Collection for alerts

# Define the alert thresholds
CALL_DURATION_THRESHOLD = 3600  # Call duration threshold in seconds (e.g., 1 hour)
FAILED_CALLS_ALERT = True  # Set to True if failed calls should trigger alerts

# Consumer processing loop
for message in consumer:
    data = message.value

    # Parse relevant fields
    customer_id = data.get('customer_id')
    agent_id = data.get('agent_id')
    call_duration_seconds = data.get('call_duration_seconds')
    is_failed = data.get('is_failed', False)
    is_ok = data.get('is_ok', True)

    # Handle the 'timestamp' field safely
    timestamp_raw = data.get('timestamp')
    try:
        timestamp = datetime.strptime(timestamp_raw, "%Y-%m-%d %H:%M:%S") if timestamp_raw else None
    except ValueError:
        print(f"Error: Invalid timestamp format in data: {data}")
        timestamp = None  # Set timestamp to None if parsing fails

    # Insert raw customer service data into MongoDB
    customer_service_collection.insert_one(data)

    # Alert for long call duration
    if call_duration_seconds and call_duration_seconds > CALL_DURATION_THRESHOLD:
        alerts_collection.insert_one({
            'type': 'Long Call Duration',
            'customer_id': customer_id,
            'call_duration_seconds': call_duration_seconds,
            'timestamp': timestamp
        })
        print(f"ALERT: Long call duration detected! Customer ID: {customer_id}")

    # Alert for failed calls
    if FAILED_CALLS_ALERT and is_failed:
        alerts_collection.insert_one({
            'type': 'Failed Call',
            'customer_id': customer_id,
            'agent_id': agent_id,
            'timestamp': timestamp
        })
        print(f"ALERT: Failed call detected! Customer ID: {customer_id}, Agent ID: {agent_id}")

    # Alert for problematic interactions
    if not is_ok:
        alerts_collection.insert_one({
            'type': 'Problematic Interaction',
            'customer_id': customer_id,
            'agent_id': agent_id,
            'timestamp': timestamp
        })
        print(f"ALERT: Problematic interaction detected! Customer ID: {customer_id}, Agent ID: {agent_id}")
