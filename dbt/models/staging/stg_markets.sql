{{ config(materialized='table') }}

SELECT
    DISTINCT market_id AS id,
    market_name AS name,
    locality_id,
    market_type_id
FROM {{ ref('stg_transactions') }}
WHERE market_id IS NOT NULL AND market_name IS NOT NULL
ORDER BY name
