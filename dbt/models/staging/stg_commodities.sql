{{ config(materialized='table') }}

SELECT
    DISTINCT commodity_id AS id,
    commodity_name AS name
FROM {{ ref('stg_transactions') }}
WHERE commodity_id IS NOT NULL AND commodity_name IS NOT NULL
ORDER BY name
