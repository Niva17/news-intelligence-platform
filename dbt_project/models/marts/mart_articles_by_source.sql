-- Mart: Articles aggregated by source
-- Business ready table for dashboards and reporting

{{ config(materialized='table') }}

SELECT
    source,
    COUNT(*) AS total_articles,
    ROUND(AVG(content_length), 2) AS avg_content_length,
    MIN(published_at) AS earliest_article,
    MAX(published_at) AS latest_article,
    COUNT(DISTINCT published_date) AS active_days
FROM {{ ref('stg_news_silver') }}
GROUP BY source
ORDER BY total_articles DESC