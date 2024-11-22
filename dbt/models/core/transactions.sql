{{ config(materialized='table') }}

SELECT
    mkt_id AS market_id,
    cm_id AS commodity_id,
    cur_id AS currency_id,
    um_id AS unit_id,
    mp_month AS month,
    mp_year AS year,
    mp_price AS price,
    mp_commoditysource AS commodity_source
FROM {{ source('core', 'raw_wfp') }}
WHERE mp_price IS NOT NULL AND mp_month IS NOT NULL AND mp_year IS NOT NULL

