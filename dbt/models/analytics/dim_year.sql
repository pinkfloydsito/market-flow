{{ config(materialized='table') }}

WITH ranked_years AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY year) AS year_id, -- Auto-increment column
        year
    FROM {{ ref('stg_transactions') }}
    WHERE year IS NOT NULL
    GROUP BY year
)
SELECT
    year_id AS id,
    year AS name
FROM ranked_years
ORDER BY year
