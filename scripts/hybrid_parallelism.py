import boto3
import json
from collections import Counter
from multiprocessing import Pool, cpu_count
import time
import requests

# AWS credentials
aws_access_key = "AKIAWCYYAKOMQDMEJSPO"
aws_secret_key = "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"
region_name = "us-east-1"
stream_name = "pipeline"

# EC2 Flask endpoints
ec2_endpoints = [
    "http://52.55.162.179:8000/process",
    "http://54.234.222.183:8000/process"
]

# Connect to Kinesis
client = boto3.client(
    'kinesis',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# Get shard iterator
response = client.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"
)['ShardIterator']

# Local Mapper
def local_mapper(record):
    try:
        data = json.loads(record)
        text = data.get("text", "")
        words = text.lower().split()
        return Counter(words)
    except Exception as e:
        print(f"Mapper error: {e}")
        return Counter()

# Local Reducer
def reducer(mapped_data):
    result = Counter()
    for c in mapped_data:
        result.update(c)
    return result

# Remote EC2 Processor
def ec2_processor(endpoint, data_chunk):
    try:
        payload = {"data": data_chunk}
        response = requests.post(endpoint, json=payload)
        return Counter(response.json())
    except Exception as e:
        print(f"Error sending to {endpoint}: {e}")
        return Counter()

if __name__ == "__main__":
    print("Starting hybrid parallelism...\n")

    while True:
        records_response = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = records_response['NextShardIterator']
        raw_records = [r['Data'].decode('utf-8') for r in records_response['Records']]

        if not raw_records:
            print("No new records...waiting.\n")
            time.sleep(2)
            continue

        # Split data for EC2s (task parallelism)
        half = len(raw_records) // 2
        chunks = [raw_records[:half], raw_records[half:]]

        # Send to EC2s
        ec2_results = []
        for endpoint, chunk in zip(ec2_endpoints, chunks):
            ec2_results.append(ec2_processor(endpoint, chunk))

        # Combine EC2 results
        combined_remote = reducer(ec2_results)

        # Local multiprocessing (data parallelism)
        with Pool(cpu_count()) as pool:
            mapped = pool.map(local_mapper, raw_records)
        combined_local = reducer(mapped)

        # Merge local + remote results
        total_counts = combined_local + combined_remote

        print("Final Word Count (Top 10):")
        for word, count in total_counts.most_common(10):
            print(f"{word}: {count}")
        print("\nWaiting for next batch...\n")

        time.sleep(5)
