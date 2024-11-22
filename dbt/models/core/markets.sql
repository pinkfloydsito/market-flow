{{ config(materialized='table') }}

SELECT
    DISTINCT mkt_id AS id,
    mkt_name AS name,
    adm1_id AS locality_id,
    pt_id AS market_type_id
FROM {{ source('core', 'raw_wfp') }}
WHERE mkt_id IS NOT NULL AND mkt_name IS NOT NULL
ORDER BY name
