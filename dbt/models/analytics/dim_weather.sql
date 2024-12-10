{{ config(materialized='table') }}

--     loc.id as locality_id,
--     loc.latitude,
--     loc.longitude,
--     wfp.mp_year AS year,
--     wfp.mp_month AS month,
--     NULL AS avg_temperature,
--     NULL AS precipitation,
--     NULL AS wind_speed

SELECT
    ROW_NUMBER() OVER () AS id,
    weather.locality_id,
    weather.latitude,
    weather.longitude,
    dim_date.id as dim_date_id,
    weather.year,
    weather.month,
    weather.avg_temperature,
    weather.precipitation,
    weather.wind_speed
FROM {{ ref('raw_weather') }} weather
JOIN {{ ref('dim_date') }} dim_date
    ON weather.year = dim_date.year
    AND weather.month = dim_date.month
ORDER BY dim_date.id, weather.locality_id
