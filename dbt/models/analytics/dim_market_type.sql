{{ config(materialized='table') }}

SELECT
    DISTINCT pt_id AS id,
    pt_name AS name
FROM {{ ref('cleaned_wfp') }}
WHERE pt_id IS NOT NULL AND pt_name IS NOT NULL
ORDER BY name
