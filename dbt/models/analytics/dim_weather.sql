{{ config(materialized='table') }}

SELECT
    weather.id as id,
    locality_id,
    dim_year.id as dim_year_id,
    dim_month.id as month_id,
    dim_date.id as dim_date_id,
    latitude,
    longitude,
    temperature,
    precipitation
FROM {{ ref('stg_weather') }} weather
JOIN {{ ref('dim_year') }} dim_year 
    ON weather.year = dim_year.name
JOIN {{ ref('dim_month') }} dim_month
    ON weather.month = dim_month.name
JOIN {{ ref('dim_date') }} dim_date
    ON dim_month.id = dim_date.month_id
    AND dim_year.id = dim_date.year_id
ORDER BY dim_year.id, dim_month.id, weather.locality_id
