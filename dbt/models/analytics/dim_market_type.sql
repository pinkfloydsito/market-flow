{{ config(materialized='table') }}

SELECT
    DISTINCT id,
    name
FROM {{ ref('stg_market_types') }}
WHERE id is not null and name is not null
ORDER BY name
