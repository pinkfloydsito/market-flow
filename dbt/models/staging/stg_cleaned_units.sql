{{ config(materialized='table') }}

WITH units AS (

    SELECT
        id AS unit_id,
        name,
        -- Extract numeric value and unit type using regular expressions
        REGEXP_EXTRACT(name, '^([0-9\.]+)') AS unit_value,
        TRIM(REGEXP_REPLACE(name, '^([0-9\.]+)', '')) AS unit_type
    FROM {{ ref('stg_units') }}  -- Replace 'units' with your actual units table

),

unit_conversions AS (

    SELECT
        unit_id,
        name,
        unit_value,
        unit_type,
        -- Convert unit_value to a numeric type safely
        CASE
            WHEN unit_value IS NOT NULL AND unit_value != '' THEN CAST(unit_value AS FLOAT)
            ELSE NULL
        END AS unit_value_numeric,
        LOWER(unit_type) AS unit_type_lower,
        -- Map unit types to their conversion factors to kg
        CASE
            WHEN LOWER(unit_type) IN ('kg', 'kgs') AND unit_value_numeric IS NOT NULL THEN unit_value_numeric
            WHEN LOWER(unit_type) IN ('g', 'gr', 'gram', 'grams') AND unit_value_numeric IS NOT NULL THEN unit_value_numeric / 1000
            WHEN LOWER(unit_type) IN ('mt', 'metric ton', 'tonne') AND unit_value_numeric IS NOT NULL THEN unit_value_numeric * 1000
            WHEN LOWER(unit_type) IN ('lb', 'lbs', 'pound', 'pounds') AND unit_value_numeric IS NOT NULL THEN unit_value_numeric * 0.453592
            WHEN LOWER(unit_type) IN ('ml', 'milliliter', 'milliliters') AND unit_value_numeric IS NOT NULL THEN unit_value_numeric / 1000000  -- Assuming density is 1 kg/L
            WHEN LOWER(unit_type) IN ('l', 'liter', 'liters') AND unit_value_numeric IS NOT NULL THEN unit_value_numeric / 1000      -- Assuming density is 1 kg/L
            -- Handle cases where unit_value is NULL or empty
            WHEN (unit_value IS NULL OR unit_value = '') AND LOWER(name) IN ('kg', 'kgs') THEN 1
            WHEN (unit_value IS NULL OR unit_value = '') AND LOWER(name) IN ('mt', 'metric ton', 'tonne') THEN 1000
            WHEN (unit_value IS NULL OR unit_value = '') AND LOWER(name) IN ('lb', 'lbs', 'pound', 'pounds') THEN 0.453592
            -- Add more conversions as needed
            ELSE NULL  -- Units that cannot be converted
        END AS unit_in_kg
    FROM units

)

SELECT
    unit_id,
    name,
    CAST(unit_in_kg AS DOUBLE PRECISION) as value_in_kg
FROM unit_conversions
