{{ config(materialized='table') }}

SELECT
    DISTINCT um_id AS id,
    um_name AS name
FROM {{ source('raw', 'wfp') }}
WHERE um_id IS NOT NULL AND um_name IS NOT NULL
ORDER BY name
