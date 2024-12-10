{{ config(materialized='table') }}

WITH dates AS (
    SELECT
        date_id,
        year,
        month
    FROM {{ ref('dim_date') }}
),

currencies AS (
    SELECT
        id
    FROM {{ ref('dim_currency') }}
),

currency_date_combinations AS (
    SELECT
        ROW_NUMBER() OVER () AS currency_value_id,
        c.id as currency_id,
        d.year,
        d.month,
        NULL AS value_usd,
        NULL AS value,
    FROM currencies c
    CROSS JOIN dates d
)

SELECT
    currency_value_id,
    currency_id,
    year,
    month,
    value_usd,
    value
FROM currency_date_combinations
ORDER BY currency_id, year, month
