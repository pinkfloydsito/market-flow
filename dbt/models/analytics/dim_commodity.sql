{{ config(materialized='table') }}

SELECT
    DISTINCT id,
    name
FROM {{ ref('commodities') }}
WHERE name IS NOT NULL
ORDER BY name
