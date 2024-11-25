{{ config(materialized='table') }}

SELECT
    DISTINCT cm_id AS id,
    cm_name AS name
FROM {{ ref('cleaned_wfp') }}
WHERE cm_id IS NOT NULL AND cm_name IS NOT NULL
ORDER BY name
