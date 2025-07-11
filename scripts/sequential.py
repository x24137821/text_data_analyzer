import boto3, json, time, csv
from collections import Counter

aws_access_key = "AKIAWCYYAKOMQDMEJSPO"
aws_secret_key = "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"
region_name = "us-east-1"
stream_name = "pipeline"

client = boto3.client(
    'kinesis',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

response = client.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"
)['ShardIterator']

def word_count(records):
    counter = Counter()
    for r in records:
        try:
            data = json.loads(r)
            words = data.get("text", "").lower().split()
            counter.update(words)
        except:
            continue
    return counter

with open("benchmark_sequential.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Batch", "TimeTaken(s)", "Records", "Throughput(rec/sec)"])

    batch_num = 1
    while batch_num <= 10:
        start = time.time()

        response = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = response["NextShardIterator"]
        records = [r["Data"].decode("utf-8") for r in response["Records"]]

        if not records:
            time.sleep(1)
            continue

        count = word_count(records)
        end = time.time()

        time_taken = end - start
        throughput = len(records) / time_taken if time_taken > 0 else 0

        print(f"Batch {batch_num}: {len(records)} records, {time_taken:.2f}s, {throughput:.2f} rec/s")
        writer.writerow([batch_num, round(time_taken, 2), len(records), round(throughput, 2)])

        batch_num += 1
        time.sleep(2)
