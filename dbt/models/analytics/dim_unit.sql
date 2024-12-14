{{ config(materialized='table') }}

SELECT
    DISTINCT unit_id as id,
    name,
    value_in_kg as value_one_kg
FROM {{ ref('stg_cleaned_units') }}
WHERE unit_id IS NOT NULL AND name IS NOT NULL
ORDER BY name
