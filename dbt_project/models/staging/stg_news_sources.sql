-- Staging model for unique news sources

{{ config(materialized='view') }}

SELECT DISTINCT
    source,
    COUNT(*) AS total_articles
FROM delta.`s3a://news-intelligence-platform-bronze/silver/news/`
WHERE source IS NOT NULL
GROUP BY source