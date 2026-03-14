import requests
import json
import time
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_TOPIC = "raw_news"
CONFLUENT_BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
CONFLUENT_API_KEY = os.getenv("CONFLUENT_API_KEY")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_API_SECRET")

def fetch_news():
    """Fetch latest news articles from NewsAPI"""
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "apiKey": NEWS_API_KEY,
        "language": "en",
        "pageSize": 20,
        "category": "technology"
    }
    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] == "ok":
        return data["articles"]
    else:
        print(f"Error fetching news: {data}")
        return []

def delivery_report(err, msg):
    """Callback to confirm message delivery"""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

def create_producer():
    """Create and return a Kafka producer"""
    producer = Producer({
        "bootstrap.servers": CONFLUENT_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": CONFLUENT_API_KEY,
        "sasl.password": CONFLUENT_API_SECRET,
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 30000
    })
    return producer

def run_producer():
    """Main function - fetch news and send to Kafka"""
    producer = create_producer()
    print("Producer started. Sending news to Kafka...")

    seen_urls = set()

    while True:
        articles = fetch_news()
        new_articles = [a for a in articles if a.get("url") not in seen_urls]
        print(f"Fetched {len(articles)} articles, {len(new_articles)} are new")

        for article in new_articles:
            message = {
                "title": article.get("title"),
                "description": article.get("description"),
                "content": article.get("content"),
                "url": article.get("url"),
                "source": article.get("source", {}).get("name"),
                "published_at": article.get("publishedAt"),
                "author": article.get("author")
            }

            producer.produce(
                KAFKA_TOPIC,
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_report
            )
            print(f"Sent: {message['title']}")
            seen_urls.add(article.get("url"))

        producer.flush()

        if new_articles:
            print(f"Batch complete. {len(seen_urls)} total articles sent so far.")
        else:
            print("No new articles. Waiting for NewsAPI to update...")

        print("Waiting 60 seconds...\n")
        time.sleep(60)

if __name__ == "__main__":
    run_producer()