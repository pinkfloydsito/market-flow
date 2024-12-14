{{ config(materialized='table') }}

SELECT DISTINCT
    CASE 
        WHEN currency_name = 'Somaliland Shilling' THEN 'SOS'
        ELSE currency_name
    END 
    AS name,
    currency_id AS id
FROM {{ ref('stg_transactions') }}
WHERE currency_id IS NOT NULL AND currency_name IS NOT NULL
ORDER BY name

