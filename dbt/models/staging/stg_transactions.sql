{{ config(materialized='table') }}

WITH transactions AS (

    SELECT
        mkt_id AS market_id,
        mkt_name AS market_name,
        pt_id AS market_type_id,
        pt_name AS market_type_name,
        cm_id AS commodity_id,
        cm_name AS commodity_name,
        adm0_id AS country_id,
        adm0_name AS country_name,
        adm1_id AS locality_id,
        adm1_name AS locality_name,
        cur_id AS currency_id,
        cur_name AS currency_name,
        um_id AS unit_id,
        CAST(mp_month AS INT) AS month,
        CAST(mp_year AS INT) AS year,
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
          ELSE price
        END AS price_per_kg
    FROM transactions_with_units

)

SELECT
    market_id,
    market_name,
    market_type_id,
    market_type_name,
    commodity_id,
    commodity_name,
    country_name,
    locality_id,
    locality_name,
    currency_id,
    currency_name,
    unit_id,
    unit_name,
    month,
    year,
    price,
    price_per_kg,
    commodity_source
FROM adjusted_transactions

