from kafka import KafkaConsumer
import json

# Replace this with the URL of the SNCF API endpoint
API_ENDPOINT = "https://api.sncf.com/v1/coverage/sncf/journeys?from=admin:7444extern&to=admin:120965extern&datetime=20170123T140151"

# Create the Kafka consumer
consumer = KafkaConsumer(
    "sncf-topic",
    bootstrap_servers=['kafka:9093'],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

# Function to process the data received from Kafka
def process_data(data):
    # Use the data received from Kafka here
    print(data)

# Process the data received from Kafka
for message in consumer:
    data = message.value
    process_data(data)
