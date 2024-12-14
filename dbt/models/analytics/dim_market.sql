{{ config(materialized='table') }}

SELECT
    DISTINCT id,
    name,
    locality_id,
    market_type_id
FROM {{ ref('stg_markets') }}
WHERE market_type_id IS NOT NULL
ORDER BY name
