{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT
        month,
        year
    FROM {{ ref('stg_transactions') }}
),

currencies AS (
    SELECT
        id,
        name
    FROM {{ ref('stg_currencies') }}
),

currency_date_combinations AS (
    SELECT
        ROW_NUMBER() OVER () AS id,
        c.id as currency_id,
        d.year,
        d.month,
        ch.value as value
    FROM currencies c
    CROSS JOIN dates d
    INNER JOIN   {{ source('raw', 'currencies_historical') }} ch
    ON ch.currency_code=c.name
    AND ch.year=d.year
    AND ch.month=d.month
)

SELECT
    id,
    currency_id,
    year,
    month,
    value
FROM currency_date_combinations
ORDER BY currency_id, year, month

