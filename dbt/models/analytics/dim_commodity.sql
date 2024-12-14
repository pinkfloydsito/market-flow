{{ config(materialized='table') }}

SELECT
    DISTINCT id,
    name
FROM {{ ref('stg_commodities') }}
WHERE name IS NOT NULL
ORDER BY name
