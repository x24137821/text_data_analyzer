import boto3
import requests
import json
import time
import csv
from multiprocessing import Pool, cpu_count
from collections import Counter

# AWS credentials and Kinesis config
aws_access_key = "AKIAWCYYAKOMQDMEJSPO"
aws_secret_key = "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"
region_name = "us-east-1"
stream_name = "pipeline"

# EC2 endpoints
ec2_endpoints = [
     "http://52.55.162.179:8000/process",
     "http://54.234.222.183:8000/process"
]

# Kinesis client
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

def local_mapper(record):
    try:
        data = json.loads(record)
        text = data.get("text", "")
        words = text.lower().split()
        return Counter(words)
    except Exception as e:
        print("Local mapper error:", e)
        return Counter()

def reducer(mapped_data):
    result = Counter()
    for c in mapped_data:
        result.update(c)
    return result

def ec2_processor(endpoint, data_chunk):
    try:
        payload = {"data": data_chunk}
        response = requests.post(endpoint, json=payload)
        return Counter(response.json())
    except Exception as e:
        print(f"EC2 processor error on {endpoint}:", e)
        return Counter()

with open("benchmark_parallel.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Batch", "TimeTaken(s)", "Records", "Throughput(rec/sec)"])

    batch_num = 1
    while batch_num <= 10:
        start = time.time()

        response = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = response['NextShardIterator']
        raw_records = [r['Data'].decode('utf-8') for r in response['Records']]

        if not raw_records:
            time.sleep(1)
            continue

        # EC2 processing
        half = len(raw_records) // 2
        chunks = [raw_records[:half], raw_records[half:]]
        ec2_results = [ec2_processor(endpoint, chunk) for endpoint, chunk in zip(ec2_endpoints, chunks)]
        combined_remote = reducer(ec2_results)

        # Local multiprocessing
        with Pool(cpu_count()) as pool:
            mapped = pool.map(local_mapper, raw_records)
        combined_local = reducer(mapped)

        total_counts = combined_local + combined_remote

        end = time.time()
        time_taken = end - start
        throughput = len(raw_records) / time_taken if time_taken > 0 else 0

        print(f"[Parallel] Batch {batch_num}: {len(raw_records)} records, {time_taken:.2f}s, {throughput:.2f} rec/s")
        writer.writerow([batch_num, round(time_taken, 2), len(raw_records), round(throughput, 2)])

        batch_num += 1
        time.sleep(2)
