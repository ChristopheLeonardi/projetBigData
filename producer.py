from kafka import KafkaProducer
import requests

# Replace this with the URL of the SNCF API endpoint
API_ENDPOINT = "http://api.sncf.com/endpoint"

# Create the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Function to fetch data from the SNCF API
def fetch_data():
    response = requests.get(API_ENDPOINT)
    data = response.json()
    return data

# Function to send the data to Kafka
def send_data(data):
    producer.send("sncf-topic", data)
    producer.flush()

# Fetch and send the data from the API
data = fetch_data()
send_data(data)
