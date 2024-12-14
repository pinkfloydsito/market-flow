{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER () AS id,
    loc.locality_id,
    weather.latitude,
    weather.longitude,
    weather.year,
    weather.month,
    weather.temperature,
    weather.precipitation
FROM {{ source('raw','weather') }} weather
INNER JOIN {{ ref('stg_localities') }} loc
    ON weather.longitude = loc.longitude
    AND weather.latitude = loc.latitude
ORDER BY loc.locality_name, weather.year, weather.month
