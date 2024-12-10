{{ config(materialized='table') }}

SELECT DISTINCT
    CASE 
        WHEN cur_name = 'Somaliland Shilling' THEN 'SOS'
        ELSE cur_name
    END 
    AS name,
    cur_id AS id
FROM {{ ref('cleaned_wfp') }}
WHERE cur_id IS NOT NULL AND cur_name IS NOT NULL
ORDER BY name
