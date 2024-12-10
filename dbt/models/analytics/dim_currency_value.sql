{{ config(materialized='table') }}

SELECT
    DISTINCT currency_value_id AS id,
    (select name from {{ ref('currencies') }} c where c.id = currency_id) AS name,
    currency_id,
    curr_value AS value,
    year,
    month
FROM {{ ref('currencies_values') }}
WHERE curr_value IS NOT NULL
ORDER BY name
