{{ config(materialized='table') }}

SELECT
    DISTINCT id,
    name
FROM {{ ref('currencies') }}
WHERE name IS NOT NULL
ORDER BY name
