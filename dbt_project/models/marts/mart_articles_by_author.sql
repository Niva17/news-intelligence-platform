-- Mart: Articles aggregated by author
-- Business ready table for author analysis

{{ config(materialized='table') }}

SELECT
    author,
    source,
    COUNT(*) AS total_articles,
    MIN(published_at) AS first_article,
    MAX(published_at) AS latest_article,
    COUNT(DISTINCT published_date) AS active_days
FROM {{ ref('stg_news_silver') }}
WHERE author != 'Unknown'
GROUP BY author, source
ORDER BY total_articles DESC