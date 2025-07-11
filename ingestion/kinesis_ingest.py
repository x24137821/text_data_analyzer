import boto3
import pandas as pd
import json
import time

# AWS credentials (USE QUOTES!)
aws_access_key = "AKIAWCYYAKOMQDMEJSPO"
aws_secret_key = "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"
region_name = "us-east-1"
stream_name = "pipeline"

# Initialize Kinesis client
kinesis = boto3.client(
    'kinesis',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# List shards and show starting positions
print("🔍 Available shards and their starting positions:")
try:
    shard_response = kinesis.list_shards(StreamName=stream_name)
    shards = shard_response.get("Shards", [])
    for shard in shards:
        print(f"ShardId: {shard['ShardId']}, StartingHashKey: {shard['HashKeyRange']['StartingHashKey']}")
except Exception as e:
    print(f"Error listing shards: {e}")

# Load fake and true datasets
df_fake = pd.read_csv("fake.csv")
df_true = pd.read_csv("true.csv")

# Add labels
df_fake["label"] = "fake"
df_true["label"] = "true"

# Combine datasets
df = pd.concat([df_fake, df_true], ignore_index=True)

# Optional: Shuffle rows
df = df.sample(frac=1).reset_index(drop=True)

# Confirm columns
print("📊 Columns:", df.columns.tolist())

# Stream records to Kinesis
for index, row in df.iterrows():
    record = {
        "title": row.get("title", ""),
        "text": row.get("text", ""),
        "label": row.get("label", "")
    }

    try:
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=str(index)  # Different key per record to spread across shards
        )
        print(f"✅ Sent record {index} | ShardId: {response['ShardId']}, SequenceNumber: {response['SequenceNumber']}")
    except Exception as e:
        print(f"❌ Error sending record {index}: {e}")

    time.sleep(0.1)

print("🎉 All records sent successfully!")
