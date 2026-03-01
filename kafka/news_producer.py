import json
import requests
import time
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_TOPIC = "raw_news"
KAFKA_BROKER = "localhost:9092"

def fetch_news():
    url = "https://newsapi.org/v2/top-headlines"
    params={
        "apiKey": NEWS_API_KEY,
        "language": "en",
        "pageSize": 20,        # fetch 20 articles at a time
        "category": "technology"
    }
    response = requests.get(url,params=params)
    data = response.json()

    if data["status"] == "ok":
        return data["articles"]
    else:
        print(f"Error fetching news: {data}")
        return []
    

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        request_timeout_ms=30000,
        api_version=(2, 5, 0)
    )
    return producer
    

def run_producer():
    producer=create_producer()
    print("producer successfully started. Sending news to Kafka..")

    seen_urls = set()

    while True:
        articles=fetch_news()
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

            producer.send(KAFKA_TOPIC,value=message)
            print(f"sent: {message['title']}")
            seen_urls.add(article.get("url"))

        producer.flush()
        if new_articles:
            print(f"Batch sent. {len(seen_urls)} total articles sent so far.")
        else:
            print("No new articles. Waiting for NewsAPI to update...")
        print("Waiting 60 seconds...")
        time.sleep(60)   

if __name__ == "__main__":
    run_producer()



