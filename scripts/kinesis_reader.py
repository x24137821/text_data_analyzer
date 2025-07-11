import boto3
import json
import time

# AWS credentials and stream info
aws_access_key = "AKIAWCYYAKOMQDMEJSPO"
aws_secret_key = "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"
region_name = "us-east-1"
stream_name = "pipeline"

# Connect to Kinesis
client = boto3.client(
    'kinesis',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# Get shard ID and shard iterator
response = client.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"  # Use "TRIM_HORIZON" to start from oldest
)["ShardIterator"]

# Poll Kinesis for new records
print("🔁 Listening for new records in stream...")

while True:
    response = client.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = response["NextShardIterator"]

    records = response["Records"]
    if records:
        for record in records:
            data = record["Data"].decode("utf-8")
            try:
                json_data = json.loads(data)
                print("✅ Received record:", json.dumps(json_data, indent=2))
            except:
                print("⚠️ Malformed JSON:", data)
    else:
        print("⏳ No new records. Waiting...\n")

    time.sleep(2)
