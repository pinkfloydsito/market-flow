{{ config(materialized='table') }}

WITH ranked_months AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY mp_month) AS month_id,
        mp_month AS month
    FROM {{ ref('cleaned_wfp') }}
    WHERE mp_month IS NOT NULL
    GROUP BY mp_month
)
SELECT
    month_id AS id,
    month AS name
FROM ranked_months
ORDER BY month
