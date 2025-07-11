import boto3
import json
import time
import re
from collections import Counter, deque
from datetime import datetime, timedelta

# AWS Configuration
aws_access_key = "AKIAWCYYAKOMQDMEJSPO"
aws_secret_key = "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"
region_name = "us-east-1"
stream_name = "pipeline"

# Setup Kinesis Client
client = boto3.client(
    "kinesis",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
)

# Get shard iterator
response = client.describe_stream(StreamName=stream_name)
shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"
)["ShardIterator"]

# Store records in a deque for sliding window (timestamp, word list)
window = deque()
WINDOW_SIZE_SECONDS = 300  # 5 minutes

def clean_and_tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

print("Starting sliding window processor (5-minute window)...\n")

while True:
    records_response = client.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = records_response["NextShardIterator"]
    now = datetime.utcnow()

    # Process new records
    for record in records_response["Records"]:
        raw_data = record["Data"].decode("utf-8")
        try:
            data = json.loads(raw_data)
            text = data.get("text", "").strip()
            if text:
                words = clean_and_tokenize(text)
                window.append((now, words))
        except Exception as e:
            print(f"Error processing record: {e}")

    # Remove old records outside sliding window
    cutoff = now - timedelta(seconds=WINDOW_SIZE_SECONDS)
    while window and window[0][0] < cutoff:
        window.popleft()

    # Aggregate word counts in the current window
    all_words = []
    for _, words in window:
        all_words.extend(words)
    word_freq = Counter(all_words)

    # Show Top 5 trending words
    print(f"\n⏳ Top 5 Trending Words (Last 5 Minutes @ {now.strftime('%H:%M:%S')} UTC):")
    for word, count in word_freq.most_common(5):
        print(f"{word}: {count}")

    print("\n--- Waiting for next batch ---\n")
    time.sleep(5)
