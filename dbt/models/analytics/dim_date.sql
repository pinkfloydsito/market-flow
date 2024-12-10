{{ config(materialized='table') }}

WITH dates AS (
    SELECT
        ROW_NUMBER() OVER () AS date_id,
        y.id as year_id,
        y.name as year,
        m.id as month_id,
        m.name as month,
    FROM {{ ref('dim_year') }} y
    CROSS JOIN {{ ref('dim_month') }} m
    WHERE y.name IS NOT NULL AND m.name IS NOT NULL
)
SELECT
    date_id as id,
    year_id,
    year,
    month_id,
    month,
FROM dates
ORDER BY year, month
