{{ config(materialized='table') }}

SELECT
    DISTINCT sch.id,
    currency_id,
    sc.name,
    sch.year,
    sch.month,
    value

FROM {{ ref('stg_currencies_historical') }} sch
INNER JOIN {{ ref('stg_currencies') }} sc
ON sc.id = currency_id
WHERE value IS NOT NULL
ORDER BY name
