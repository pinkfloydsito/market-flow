{{ config(materialized='table') }}

WITH transactions AS (

    SELECT
        mkt_id AS market_id,
        cm_id AS commodity_id,
        cur_id AS currency_id,
        um_id AS unit_id,
        mp_month AS month,
        mp_year AS year,
        CAST(mp_price AS DOUBLE PRECISION) AS price,
        mp_commoditysource AS commodity_source
    FROM {{ source('raw', 'wfp') }}
    WHERE mp_price IS NOT NULL AND mp_month IS NOT NULL AND mp_year IS NOT NULL
),

units AS (

    SELECT
        unit_id,
        name AS unit_name,
        value_in_kg
    FROM {{ ref('stg_cleaned_units') }}

),

transactions_with_units AS (

    SELECT
        t.*,
        u.unit_name,
        u.value_in_kg
    FROM transactions t
    LEFT JOIN units u ON t.unit_id = u.unit_id

),

adjusted_transactions AS (

    SELECT
        *,
        CASE
        WHEN value_in_kg IS NOT NULL AND value_in_kg > 0 
          THEN price / value_in_kg
          ELSE NULL
        END AS price_per_kg
    FROM transactions_with_units

)

SELECT
    market_id,
    commodity_id,
    currency_id,
    unit_id,
    unit_name,
    month,
    year,
    price,
    price_per_kg,
    commodity_source
FROM adjusted_transactions

