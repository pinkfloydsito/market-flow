{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT
        mp_month as month,
        mp_year as year
    FROM {{ ref('cleaned_wfp') }}
),

currencies AS (
    SELECT
        id
    FROM {{ ref('currencies') }}
),

currency_date_combinations AS (
    SELECT
        ROW_NUMBER() OVER () AS currency_value_id,
        c.id as currency_id,
        d.year,
        d.month,
        CAST(NULL AS FLOAT) AS curr_value
    FROM currencies c
    CROSS JOIN dates d
)

SELECT
    currency_value_id,
    currency_id,
    year,
    month,
    curr_value
FROM currency_date_combinations
ORDER BY currency_id, year, month

