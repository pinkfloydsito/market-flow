{{ config(materialized='table') }}

SELECT
    DISTINCT cur_id AS id,
    cur_name AS name
FROM {{ ref('cleaned_wfp') }}
WHERE cur_id IS NOT NULL AND cur_name IS NOT NULL
ORDER BY name
