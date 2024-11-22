{{ config(materialized='table') }}

SELECT
    DISTINCT adm1_id AS id,
    adm1_name AS name,
    adm0_id AS country_id
FROM {{ source('core', 'raw_wfp') }}
WHERE adm1_id IS NOT NULL AND adm1_name IS NOT NULL
ORDER BY name
