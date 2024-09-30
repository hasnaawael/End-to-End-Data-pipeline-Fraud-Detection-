from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import csv
import json  # Import json module to handle JSON serialization
from datetime import datetime

topicname = 'Transaction-topic'
BROKERS = 'b-1.mskkafkacluster.og8c84.c2.kafka.eu-north-1.amazonaws.com:9098'
region = 'eu-north-1'

# MSK Token Provider class
class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

tp = MSKTokenProvider()

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize to JSON format
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
)

# Read data from CSV file and convert to JSONL format
def read_csv_file(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row  # Use a generator to read the file in chunks


# Continuously send data from CSV file
def send_data_from_csv(file_path):
    for data in read_csv_file(file_path):
        try:
            # Print data and the current time (hour, minute, second)
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"Sending data at {current_time}: {json.dumps(data)}")
            
            # Send data to Kafka topic
            future = producer.send(topicname, value=data)  # Send as JSON object
            producer.flush()
            record_metadata = future.get(timeout=10)
        except Exception as e:
            print(e)

        
# Update the path to your CSV file here
csv_file_path = 'Data_Set_2.csv'
send_data_from_csv(csv_file_path)
