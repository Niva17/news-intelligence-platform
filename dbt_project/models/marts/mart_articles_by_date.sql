-- Mart: Articles aggregated by date
-- Business ready table for time series analysis

{{ config(materialized='table') }}

SELECT
    published_date,
    published_year,
    published_month,
    COUNT(*) AS total_articles,
    COUNT(DISTINCT source) AS unique_sources,
    COUNT(DISTINCT author) AS unique_authors
FROM {{ ref('stg_news_silver') }}
GROUP BY published_date, published_year, published_month
ORDER BY published_date DESC