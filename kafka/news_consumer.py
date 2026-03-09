import json
import boto3
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

KAFKA_TOPIC = "raw_news"
KAFKA_BROKER = "127.0.0.1:9092"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def create_consumer():
    """Create and return a Kafka consumer"""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "news-consumer-group",
        "auto.offset.reset": "latest",
        "socket.timeout.ms": 10000
    })
    consumer.subscribe([KAFKA_TOPIC])
    return consumer

def create_s3_client():
    """Create and return an S3 client"""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1"
    )
    return s3

def get_existing_urls(s3_client):
    """Fetch all URLs already stored in S3 by reading file contents"""
    existing_urls = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix="bronze/news/")

        for page in pages:
            for obj in page.get("Contents", []):
                try:
                    # First try metadata
                    metadata = s3_client.head_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=obj["Key"]
                    )
                    url = metadata.get("Metadata", {}).get("article_url")

                    if url:
                        existing_urls.add(url)
                    else:
                        # Fall back to reading file content
                        response = s3_client.get_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=obj["Key"]
                        )
                        article = json.loads(response["Body"].read().decode("utf-8"))
                        if article.get("url"):
                            existing_urls.add(article["url"])
                except Exception as e:
                    print(f"Skipping file {obj['Key']}: {e}")

    except Exception as e:
        print(f"Error fetching existing URLs: {e}")

    return existing_urls

def upload_to_s3(s3_client, article):
    """Upload a single article to S3 as a JSON file with URL metadata"""
    timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
    safe_title = article.get("title", "untitled")[:50]\
        .replace("/", "-")\
        .replace(" ", "_")\
        .replace(":", "-")\
        .replace("'", "")\
        .replace(",", "")\
        .replace("?", "")\
        .replace("!", "")
    filename = f"bronze/news/{timestamp}_{safe_title}.json"

    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=filename,
        Body=json.dumps(article),
        ContentType="application/json",
        Metadata={
            "article_url": article.get("url", "")[:500]
        }
    )
    return filename

def run_consumer():
    """Main function - consume from Kafka and save to S3"""
    consumer = create_consumer()
    s3_client = create_s3_client()

    print("Loading existing URLs from S3...")
    seen_urls = get_existing_urls(s3_client)
    print(f"Found {len(seen_urls)} existing articles in S3")
    print("Consumer started. Listening for messages...")

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        article = json.loads(msg.value().decode("utf-8"))
        article_url = article.get("url")

        if article_url in seen_urls:
            print(f"Skipping duplicate: {article.get('title')}")
            continue

        print(f"Received: {article.get('title')}")

        try:
            filename = upload_to_s3(s3_client, article)
            seen_urls.add(article_url)
            print(f"Uploaded to S3: {filename}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")

if __name__ == "__main__":
    run_consumer()