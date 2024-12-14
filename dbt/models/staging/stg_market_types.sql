{{ config(materialized='table') }}

SELECT
    DISTINCT market_type_id AS id,
    market_type_name AS name
FROM {{ ref('stg_transactions') }}
WHERE market_type_id IS NOT NULL AND market_type_name IS NOT NULL
ORDER BY name
