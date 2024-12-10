-- Initial state for weather data
{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER () AS id,
    loc.id as locality_id,
    loc.latitude,
    loc.longitude,
    wfp.mp_year AS year,
    wfp.mp_month AS month,
    NULL AS avg_temperature,
    NULL AS precipitation,
    NULL AS wind_speed
FROM {{ ref('cleaned_wfp') }} wfp
INNER JOIN {{ ref('markets') }} mkt
    ON wfp.mkt_id = mkt.id
INNER JOIN {{ ref('localities') }} loc
    ON mkt.locality_id = loc.id
WHERE 
    wfp.mp_price IS NOT NULL
    AND wfp.mp_month IS NOT NULL
    AND wfp.mp_year IS NOT NULL
    AND loc.latitude IS NOT NULL
    AND loc.longitude IS NOT NULL
GROUP BY loc.id, loc.latitude, loc.longitude, wfp.mp_year, wfp.mp_month
HAVING COUNT(*) > 1

