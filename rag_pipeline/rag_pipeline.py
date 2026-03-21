import os
from dotenv import load_dotenv
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
from groq import Groq

load_dotenv()

# ── Clients ──
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# ── Config ──
INDEX_NAME = "news-intelligence"

def get_index():
    """Connect to Pinecone index"""
    return pc.Index(INDEX_NAME)

def search_similar_articles(query, top_k=5):
    """Search Pinecone for articles similar to the query"""
    index = get_index()

    # Convert query to embedding using HuggingFace
    query_embedding = embedding_model.encode(query).tolist()

    # Search Pinecone for similar vectors
    results = index.query(
        vector=query_embedding,
        top_k=top_k,
        include_metadata=True
    )

    return results["matches"]

def build_context(matches):
    """Build context string from search results"""
    context_parts = []
    for i, match in enumerate(matches, 1):
        metadata = match["metadata"]
        context_parts.append(f"""
Article {i}:
Title: {metadata.get('title', '')}
Source: {metadata.get('source', '')}
Published: {metadata.get('published_at', '')}
Description: {metadata.get('description', '')}
URL: {metadata.get('url', '')}
""")
    return "\n".join(context_parts)

def answer_question(question):
    """Main RAG function - search relevant articles + generate answer"""

    # Step 1 — Convert question to vector and search Pinecone
    matches = search_similar_articles(question, top_k=5)

    if not matches:
        return "No relevant articles found.", []

    # Step 2 — Build context from matching articles
    context = build_context(matches)

    # Step 3 — Generate answer using Groq (Llama 3)
    prompt = f"""You are a helpful news assistant. Using the news articles provided below,
answer the user's question accurately and concisely.
If the articles don't contain enough information, say so honestly.
Always mention which sources you used in your answer.

News Articles:
{context}

Question: {question}

Answer:"""

    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
        temperature=0.3
    )

    answer = response.choices[0].message.content
    return answer, matches

if __name__ == "__main__":
    # Test the RAG pipeline
    test_questions = [
        "What is the latest news about Apple?",
        "What's happening with AI this week?",
        "Any news about gaming?"
    ]

    for question in test_questions:
        print(f"\nQuestion: {question}")
        answer, matches = answer_question(question)
        print(f"Answer: {answer}")
        print(f"Sources used: {[m['metadata']['source'] for m in matches]}")
        print("-" * 50)