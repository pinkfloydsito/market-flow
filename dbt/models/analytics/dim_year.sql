{{ config(materialized='table') }}

WITH ranked_years AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY mp_year) AS year_id, -- Auto-increment column
        mp_year AS year
    FROM {{ ref('cleaned_wfp') }}
    WHERE mp_year IS NOT NULL
    GROUP BY mp_year
)
SELECT
    year_id AS id,
    year AS name
FROM ranked_years
ORDER BY year
