{{ config(materialized='view') }}

SELECT
    title,
    description,
    url,
    source,
    author,
    published_at,
    content_length,
    has_content,
    CAST(published_at AS DATE) AS published_date,
    YEAR(published_at) AS published_year,
    MONTH(published_at) AS published_month
FROM delta.`s3a://news-intelligence-platform-bronze/silver/news/`
WHERE title IS NOT NULL
AND url IS NOT NULL