import boto3
import json
import time
import random
from faker import Faker

fake = Faker()


# Define the stream name and the data to put into the stream
#  Run this script through GIT BASH as AWS conguraiotn already done using access_key and access_id

# Configuration - Set your stream name and AWS region
STREAM_NAME="st-raw-data-stream"
REGION="eu-west-2"

# Set up the Kinesis client
kinesis = boto3.client('kinesis', region_name=REGION)  # Replace with your AWS region


for i in range(1,20):
    partition_key = f"partition-key-{i}"  # Create a partition key that changes iteratively
    data = {
                  "user_id" : str(random.randint(1,5)),
                  "date" : fake.date(),
                  "event" : fake.sentence(nb_words=3),
                  "location" : fake.city()
     }

    print(data)
    # Serialize dictionary to JSON string
    json_data = json.dumps(data)

    # Convert JSON string to bytes
    #json_data_bytes = json_data.encode('utf-8')

    # Put the record into Kinesis stream
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        #Data=json_data_bytes,  # Data can be a string or binary data
        Data=json_data,  # Data can be a string or binary data
        PartitionKey='partition_key'  # The partition key is used to group records
    )

# Print the response from the Kinesis service
print(f"Record {i} sent with partition key {partition_key}")
