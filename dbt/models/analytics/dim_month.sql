{{ config(materialized='table') }}

WITH ranked_months AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY month) AS month_id,
        month
    FROM {{ ref('stg_transactions') }}
    WHERE month IS NOT NULL
    GROUP BY month
)
SELECT
    month_id AS id,
    month AS name
FROM ranked_months
ORDER BY month
