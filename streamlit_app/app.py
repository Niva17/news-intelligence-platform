import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from rag_pipeline.rag_pipeline import answer_question, search_similar_articles

# ── Page Config ──
st.set_page_config(
    page_title="News Intelligence Platform",
    page_icon="🗞️",
    layout="wide"
)

# ── Header ──
st.title("🗞️ AI-Powered News Intelligence Platform")
st.markdown("Ask anything about the latest tech news — powered by RAG, HuggingFace & Groq")
st.divider()

# ── Sidebar ──
with st.sidebar:
    st.header("⚙️ Settings")
    top_k = st.slider("Number of articles to search", min_value=1, max_value=10, value=5)
    st.divider()
    st.header("📊 About")
    st.markdown("""
    **Pipeline:**
    - 📡 Kafka → Confluent Cloud
    - 🔥 Spark Streaming → Delta Lake
    - 🧹 Bronze → Silver → Gold
    - 🔍 HuggingFace Embeddings
    - 📌 Pinecone Vector DB
    - 🤖 Groq (Llama 3.1)
    """)
    st.divider()
    st.markdown("Built by **Niva Donga**")
    st.markdown("[GitHub](https://github.com/Niva17/news-intelligence-platform)")

# ── Chat History ──
if "messages" not in st.session_state:
    st.session_state.messages = []

# ── Display chat history ──
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("sources"):
            with st.expander("📰 Sources"):
                for source in message["sources"]:
                    st.markdown(f"- [{source['title']}]({source['url']}) — **{source['source']}**")

# ── Chat Input ──
if prompt := st.chat_input("Ask about the latest tech news..."):

    # Add user message to history
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate answer
    with st.chat_message("assistant"):
        with st.spinner("Searching news articles and generating answer..."):
            try:
                answer, matches = answer_question(prompt)

                # Display answer
                st.markdown(answer)

                # Prepare sources
                sources = [
                    {
                        "title": m["metadata"].get("title", ""),
                        "url": m["metadata"].get("url", ""),
                        "source": m["metadata"].get("source", ""),
                        "published_at": m["metadata"].get("published_at", "")
                    }
                    for m in matches
                ]

                # Display sources
                with st.expander("📰 Sources"):
                    for source in sources:
                        st.markdown(f"- [{source['title']}]({source['url']}) — **{source['source']}**")

                # Add assistant message to history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer,
                    "sources": sources
                })

            except Exception as e:
                st.error(f"Error: {e}")

# ── Sample Questions ──
if not st.session_state.messages:
    st.markdown("### 💡 Try asking:")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("What's the latest news about Apple?")
    with col2:
        st.info("What's happening in gaming this week?")
    with col3:
        st.info("Any AI news today?")