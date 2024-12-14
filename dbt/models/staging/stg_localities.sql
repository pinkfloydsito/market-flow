{{ config(materialized='table') }}

WITH coordinates_with_countries AS (
    SELECT 
        c.id as country_id,
        c.name as country_name,
        coords.locality,
        CAST(coords.latitude AS DOUBLE PRECISION) as latitude,
        CAST(coords.longitude AS DOUBLE PRECISION) as longitude
    FROM {{ ref('stg_countries') }} c
    INNER JOIN {{ source('raw', 'coordinates') }} coords
        ON coords.country = c.name
),
transactions_localities AS (
    SELECT DISTINCT
        locality_id,
        locality_name,
        country_name
    FROM {{ ref('stg_transactions') }}
    WHERE locality_id IS NOT NULL 
        AND country_name IS NOT NULL
)

SELECT 
    t.locality_id,
    t.locality_name,
    c.country_id,
    c.latitude,
    c.longitude
FROM transactions_localities t
LEFT JOIN coordinates_with_countries c
    ON LOWER(TRIM(c.locality)) = LOWER(TRIM(t.locality_name))
    AND LOWER(TRIM(c.country_name)) = LOWER(TRIM(t.country_name))
ORDER BY t.locality_name
