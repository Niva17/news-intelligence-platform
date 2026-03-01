import json
import os
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

KAFKA_TOPIC="raw_news"
KAFKA_BROKER="localhost:9092"
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

def create_consumer():

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x:json.loads(x.decode("UTF-8")),
        auto_offset_reset="latest",   # read from beginning if no offset found
        group_id="news-consumer-group",
        api_version=(2, 5, 0)
    )
    return consumer

def create_s3_client():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name='us-east-1'
    )
    return s3

def upload_to_s3(s3_client, article):
    """Upload a single article to S3 as a JSON file"""
    # Create a unique filename using timestamp and title
    timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
    safe_title = article.get("title", "untitled")[:50].replace("/", "-").replace(" ", "_")
    filename = f"bronze/news/{timestamp}_{safe_title}.json"

    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=filename,
        Body=json.dumps(article),
        ContentType="application/json"
    )
    return filename

def run_consumer():
    """Main function - consume from Kafka and save to S3"""
    consumer = create_consumer()
    s3_client = create_s3_client()
    print("Consumer started. Listening for messages...")

    for message in consumer:
        article = message.value
        print(f"Received: {article.get('title')}")

        try:
            filename = upload_to_s3(s3_client, article)
            print(f"Uploaded to S3: {filename}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")

if __name__ == "__main__":
    run_consumer()
