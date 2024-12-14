{{ config(materialized='table') }}

SELECT 
    locality_id as id,
    locality_name as name,
    country_id,
    latitude,
    longitude
FROM {{ ref('stg_localities') }}
ORDER BY locality_name
