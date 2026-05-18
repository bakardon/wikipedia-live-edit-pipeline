# Import Kafka consumer
from kafka import KafkaConsumer

# Import JSON library
import json

# Create Kafka consumer
consumer = KafkaConsumer(
    'students',

    bootstrap_servers='127.0.0.1:9094',

    auto_offset_reset='earliest',

    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for student data...\n")

# Continuously read messages
for message in consumer:

    # Extract student data
    student = message.value

    # Calculate average
    average = (student["math"] + student["physics"]) / 2

    # Print result
    print(f"Student: {student['name']}")
    print(f"Math: {student['math']}")
    print(f"Physics: {student['physics']}")
    print(f"Average: {average}")
    print("-" * 30)