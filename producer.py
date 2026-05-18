# Import Kafka producer library
from kafka import KafkaProducer

# Used for converting Python dictionary to JSON
import json

# Used to add delay between messages
import time


# Create Kafka producer
producer = KafkaProducer(
    
    # Kafka server address
    bootstrap_servers='localhost:9094',

    # Convert Python dictionary into JSON bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Sample student marks data
students = [

    {
        "name": "Student1",
        "math": 55,
        "physics": 50
    },

    {
        "name": "Student2",
        "math": 65,
        "physics": 60
    }
]


# Send each student record to Kafka topic
for student in students:

    # Send data to topic named "students"
    producer.send("students", student)

    # Print confirmation message
    print(f"Sent: {student}")

    # Wait 1 second before next message
    time.sleep(1)


# Ensure all messages are sent successfully
producer.flush()