{{ config(materialized='table') }}

WITH localities AS (
    -- Get all unique localities
    SELECT
        loc.id AS locality_id,
        loc.latitude,
        loc.longitude
    FROM {{ ref('dim_locality') }} loc
    WHERE loc.latitude IS NOT NULL AND loc.longitude IS NOT NULL
),

dates AS (
    SELECT
        d.date_id AS dim_date_id,
        d.year,
        d.month
    FROM {{ ref('dim_date') }} d
),

locality_date_combinations AS (
    SELECT
        l.locality_id,
        l.latitude,
        l.longitude,
        d.dim_date_id,
        d.year,
        d.month
    FROM localities l
    CROSS JOIN dates d
),

weather_data AS (
    -- Try to match existing weather data from cleaned_wfp via dim_market
    SELECT
        loc_date.locality_id,
        loc_date.latitude,
        loc_date.longitude,
        loc_date.dim_date_id,
        loc_date.year,
        loc_date.month,
        NULL AS avg_temperature, -- Placeholder for weather data
        NULL AS precipitation,
        NULL AS wind_speed
    FROM locality_date_combinations loc_date
    LEFT JOIN {{ ref('dim_market') }} mkt
        ON loc_date.locality_id = mkt.locality_id
    LEFT JOIN {{ ref('cleaned_wfp') }} wfp
        ON mkt.id = wfp.mkt_id
        AND loc_date.year = wfp.mp_year
        AND loc_date.month = wfp.mp_month
    GROUP BY loc_date.locality_id, loc_date.latitude, loc_date.longitude, loc_date.dim_date_id, loc_date.year, loc_date.month
)

SELECT
    ROW_NUMBER() OVER () AS id,
    locality_id,
    latitude,
    longitude,
    dim_date_id,
    year,
    month,
    avg_temperature,
    precipitation,
    wind_speed
FROM weather_data
ORDER BY dim_date_id, locality_id
