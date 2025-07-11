import boto3
import json
import time
import re
from collections import Counter
from multiprocessing import Pool, cpu_count
from textblob import TextBlob

# AWS credentials
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

# Get shard ID and iterator
response = client.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"
)["ShardIterator"]

# --- MAP FUNCTION ---
def mapper(record):
    try:
        data = json.loads(record)
        text = data.get("text", "").strip()
        if not text:
            return {"word_count": Counter(), "sentiment": 0.0}

        # Word Count
        words = re.findall(r'\b\w+\b', text.lower())
        word_count = Counter(words)

        # Sentiment Analysis
        sentiment_score = TextBlob(text).sentiment.polarity  # -1 to 1

        return {"word_count": word_count, "sentiment": sentiment_score}
    except Exception as e:
        print(f"Error processing record: {e}")
        return {"word_count": Counter(), "sentiment": 0.0}

# --- REDUCE FUNCTION ---
def reducer(mapped_results):
    total_counts = Counter()
    total_sentiment = 0.0
    valid_count = 0

    for result in mapped_results:
        total_counts.update(result["word_count"])
        total_sentiment += result["sentiment"]
        valid_count += 1

    average_sentiment = total_sentiment / valid_count if valid_count > 0 else 0.0
    return total_counts, average_sentiment

if __name__ == '__main__':
    print("Starting Kinesis Word Count + Sentiment Analysis with multiprocessing...\n")

    while True:
        records_response = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = records_response["NextShardIterator"]
        raw_records = [r["Data"].decode("utf-8") for r in records_response["Records"]]

        if not raw_records:
            print("No new records... waiting.")
            time.sleep(2)
            continue

        # Parallel Map Phase
        with Pool(processes=cpu_count()) as pool:
            mapped_results = pool.map(mapper, raw_records)

        # Reduce Phase
        final_word_counts, avg_sentiment = reducer(mapped_results)

        # --- Save Top Words to word_freq.json ---
        top_words = dict(final_word_counts.most_common(20))
        with open("word_freq.json", "w") as wf:
            json.dump(top_words, wf)

        # --- Save Sentiment to sentiment.json ---
        sentiment_label = (
            "Positive" if avg_sentiment > 0.1 else
            "Negative" if avg_sentiment < -0.1 else
            "Neutral"
        )
        with open("sentiment.json", "w") as sf:
            json.dump({"score": avg_sentiment, "label": sentiment_label}, sf)

        # Print to terminal
        print("\nWord Count Result (Top 10):")
        for word, count in final_word_counts.most_common(10):
            print(f"{word}: {count}")

        print(f"\nAverage Sentiment Score: {avg_sentiment:.3f}")
        print(f"Overall Sentiment: {sentiment_label}")
        print("\nWaiting for next batch...\n")
        time.sleep(5)
