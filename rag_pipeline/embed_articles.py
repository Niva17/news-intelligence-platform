import os
import pandas as pd
from dotenv import load_dotenv
from pinecone import Pinecone
from deltalake import DeltaTable
from sentence_transformers import SentenceTransformer

load_dotenv()

# ── Clients ──
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
model = SentenceTransformer("all-MiniLM-L6-v2")

# ── Config ──
INDEX_NAME = "news-intelligence"
SILVER_PATH = "s3://news-intelligence-platform-bronze/silver/news/"

def connect_to_pinecone():
    """Connect to existing Pinecone index"""
    index = pc.Index(INDEX_NAME)
    print(f"Connected to Pinecone index: {INDEX_NAME}")
    return index

def get_articles_from_silver():
    """Read clean articles directly from Silver Delta table on S3"""
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": "us-east-1"
    }

    dt = DeltaTable(
        SILVER_PATH,
        storage_options=storage_options
    )

    df = dt.to_pandas()
    print(f"Loaded {len(df)} articles from Silver Delta table")
    return df.to_dict("records")

def prepare_text(article):
    """Combine article fields into a single text for embedding"""
    parts = []
    if article.get("title"):
        parts.append(f"Title: {article['title']}")
    if article.get("description"):
        parts.append(f"Description: {article['description']}")
    if article.get("content"):
        parts.append(f"Content: {article['content']}")
    return " ".join(parts)

def embed_and_store(index, articles):
    """Generate embeddings and store in Pinecone"""
    try:
        stats = index.describe_index_stats()
        print(f"Existing vectors in Pinecone: {stats['total_vector_count']}")
    except Exception as e:
        print(f"Could not fetch stats: {e}")

    batch_size = 10
    print(f"Embedding {len(articles)} articles...")

    for i in range(0, len(articles), batch_size):
        batch = articles[i:i + batch_size]
        vectors = []

        for article in batch:
            try:
                if not article.get("url") or not article.get("title"):
                    continue

                # Generate embedding using HuggingFace
                text = prepare_text(article)
                embedding = model.encode(text).tolist()

                # Convert timestamp to string if needed
                published_at = article.get("published_at", "")
                if hasattr(published_at, "isoformat"):
                    published_at = published_at.isoformat()

                vectors.append({
                    "id": str(article["url"]),
                    "values": embedding,
                    "metadata": {
                        "title": str(article.get("title", "")),
                        "description": str(article.get("description", "") or ""),
                        "url": str(article.get("url", "")),
                        "source": str(article.get("source", "") or ""),
                        "published_at": str(published_at),
                        "author": str(article.get("author", "") or "")
                    }
                })
                print(f"Embedded: {article.get('title', '')[:60]}")

            except Exception as e:
                print(f"Error embedding article: {e}")

        if vectors:
            index.upsert(vectors=vectors)
            print(f"Stored batch of {len(vectors)} vectors in Pinecone")

    final_stats = index.describe_index_stats()
    print(f"Done! Total vectors in Pinecone: {final_stats['total_vector_count']}")

def main():
    print("Starting embedding pipeline...")
    print("Loading HuggingFace model (downloading if first time)...")
    index = connect_to_pinecone()
    articles = get_articles_from_silver()
    embed_and_store(index, articles)
    print("Embedding pipeline complete!")

if __name__ == "__main__":
    main()