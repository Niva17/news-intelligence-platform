# 🗞️ AI-Powered News Intelligence Platform

A production-grade, end-to-end data engineering pipeline that streams real-time news articles, processes them through a medallion architecture, and enables AI-powered semantic search using RAG (Retrieval-Augmented Generation).

---

## 🏗️ Architecture

```
NewsAPI
   ↓
Kafka Producer (Python)
   ↓
Confluent Cloud (Managed Kafka)
   ↓
Spark Streaming (Databricks) → Bronze Delta Table (Raw)
   ↓
Spark Batch (Databricks) → Silver Delta Table (Cleaned)
   ↓
   ├── Spark Batch → Gold Delta Tables (Aggregated)
   └── dbt Models → Data Marts (Tested + Documented)
   ↓
Pinecone (Vector DB) ← OpenAI Embeddings
   ↓
LangChain RAG Pipeline
   ↓
Streamlit Chatbot UI
```

---

## 🛠️ Tech Stack

| Layer              | Technology                              |
| ------------------ | --------------------------------------- |
| **Streaming**      | Apache Kafka (Confluent Cloud)          |
| **Processing**     | Apache Spark 4.1, Databricks Serverless |
| **Storage**        | AWS S3, Delta Lake                      |
| **Transformation** | dbt (data build tool)                   |
| **Vector DB**      | Pinecone                                |
| **AI/LLM**         | OpenAI, LangChain                       |
| **Orchestration**  | Apache Airflow (planned)                |
| **Cloud**          | AWS (S3, MSK)                           |
| **Language**       | Python 3.9                              |

---

## 📁 Project Structure

```
news-intelligence-platform/
│
├── kafka/
│   ├── docker-compose.yml          # Local Kafka setup
│   ├── news_producer.py            # Fetches news from NewsAPI → Confluent Cloud
│   └── news_consumer_local.py      # Legacy local Kafka consumer
│
├── databricks/
│   ├── 01_bronze_to_silver_gold.py # Batch: Bronze → Silver → Gold Delta tables
│   └── 02_spark_streaming.py       # Streaming: Confluent Cloud → Bronze Delta
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_news_silver.sql     # Staging view from Silver Delta
│   │   │   ├── stg_news_sources.sql    # Staging view for sources
│   │   │   └── schema.yml             # dbt tests + documentation
│   │   └── marts/
│   │       ├── mart_articles_by_source.sql
│   │       ├── mart_articles_by_date.sql
│   │       └── mart_articles_by_author.sql
│   └── dbt_project.yml
│
├── rag_pipeline/                   # Coming soon
├── streamlit_app/                  # Coming soon
├── .env.example                    # Environment variables template
├── requirements.txt
└── README.md
```

---

## 🗄️ Data Architecture — Medallion Layers

### 🥉 Bronze Layer

- **Path:** `s3://bucket/bronze/news_delta/`
- **Format:** Delta Lake
- **Content:** Raw, unprocessed articles exactly as received from NewsAPI via Kafka
- **Purpose:** Source of truth — never modified, always replayable

### 🥈 Silver Layer

- **Path:** `s3://bucket/silver/news/`
- **Format:** Delta Lake
- **Transformations:**
  - Deduplicated by URL
  - Null filtering (title, URL required)
  - Timestamp parsing
  - Author/source null handling
  - Content length calculation
- **Purpose:** Clean, analysis-ready articles

### 🥇 Gold Layer

- **Path:** `s3://bucket/gold/`
- **Format:** Delta Lake
- **Tables:**
  - `by_source` — article counts, avg content length per source
  - `by_date` — daily article and author counts
  - `by_author` — articles per author per source
- **Purpose:** Business-ready aggregations

### 📊 dbt Marts

- **Catalog:** Databricks Unity Catalog
- **Schema:** `news_intelligence`
- **Models:**
  - `stg_news_silver` — staging view with enriched columns
  - `stg_news_sources` — unique sources with counts
  - `mart_articles_by_source` — production-ready source analytics
  - `mart_articles_by_date` — time series analysis
  - `mart_articles_by_author` — author performance metrics

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.9+
- Docker Desktop
- AWS Account
- Confluent Cloud Account
- Databricks Account (AWS)

### 1. Clone the repository

```bash
git clone https://github.com/Niva17/news-intelligence-platform.git
cd news-intelligence-platform
```

### 2. Create virtual environment

```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
# Fill in your credentials in .env
```

Required variables:

```
NEWS_API_KEY=
CONFLUENT_BOOTSTRAP_SERVERS=
CONFLUENT_API_KEY=
CONFLUENT_API_SECRET=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_BUCKET_NAME=
OPENAI_API_KEY=
PINECONE_API_KEY=
```

---

## 🚀 Running the Pipeline

### Step 1 — Start Kafka Producer

```bash
python kafka/news_producer.py
```

### Step 2 — Run Spark Streaming (Databricks)

Run `02_spark_streaming` notebook in Databricks to ingest from Confluent Cloud → Bronze Delta

### Step 3 — Run Batch Processing (Databricks)

Run `01_bronze_to_silver_gold` notebook to process Bronze → Silver → Gold

### Step 4 — Run dbt transformations

```bash
cd dbt_project
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

## ✅ dbt Data Quality Tests

| Test     | Column       | Status |
| -------- | ------------ | ------ |
| not_null | url          | ✅     |
| not_null | title        | ✅     |
| not_null | source       | ✅     |
| not_null | published_at | ✅     |
| unique   | url          | ✅     |
| unique   | source       | ✅     |

---

## 🔑 Key Concepts Demonstrated

- **Medallion Architecture** — Bronze/Silver/Gold layered data lake
- **Delta Lake** — ACID transactions, time travel, schema enforcement
- **Spark Structured Streaming** — real-time data ingestion with checkpointing
- **Kafka Offsets & Checkpointing** — fault-tolerant streaming with no data loss
- **dbt** — SQL transformations with testing, documentation and lineage
- **MERGE/Upsert** — idempotent pipeline runs with no duplicates
- **Watermarking** — handling late-arriving data in streams
- **RAG Pipeline** — AI-powered semantic search (coming soon)

---

## 🗺️ Roadmap

- [x] Phase 1 — Kafka streaming pipeline
- [x] Phase 2 — Databricks + Delta Lake medallion architecture
- [x] Phase 3 — dbt transformations, tests and documentation
- [ ] Phase 4 — Pinecone vector store + OpenAI embeddings
- [ ] Phase 5 — LangChain RAG pipeline
- [ ] Phase 6 — Streamlit chatbot UI
- [ ] Phase 7 — Airflow orchestration

---

## 👩‍💻 Author

Built as a portfolio project to demonstrate modern data engineering skills including streaming, lakehouse architecture, and AI pipeline development.

---

## 📄 License

MIT License
