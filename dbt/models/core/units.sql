{{ config(materialized='table') }}

SELECT
    DISTINCT um_id AS id,
    um_name AS name
FROM {{ source('core', 'raw_wfp') }}
WHERE um_id IS NOT NULL AND um_name IS NOT NULL
ORDER BY name
