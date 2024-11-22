{{ config(materialized='table') }}

SELECT
    DISTINCT adm1_id AS id,
    adm1_name AS name,
    (select id from countries where name={{ source('core', 'raw_wfp') }}.adm0_name limit 1) AS country_id
FROM {{ source('core', 'raw_wfp') }}
WHERE adm1_id IS NOT NULL AND adm1_name IS NOT NULL
ORDER BY name
