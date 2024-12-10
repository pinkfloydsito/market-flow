{{ config(materialized='table') }}

WITH base_transactions AS (
    SELECT
        wfp.mkt_id AS market_id,
        wfp.cm_id AS commodity_id,
        wfp.cur_id AS currency_id,
        wfp.um_id AS unit_id,
        loc.id AS locality_id,
        wfp.mp_year AS year,
        wfp.mp_month AS month,
        wfp.mp_price AS price,
        wfp.mp_commoditysource AS commodity_source,
        u.name as unit_name,
        u.unit_in_kg
    FROM {{ ref('cleaned_wfp') }} wfp
    INNER JOIN {{ ref('dim_market') }} mkt
        ON wfp.mkt_id = mkt.id
    INNER JOIN {{ ref('dim_locality') }} loc
        ON mkt.locality_id = loc.id
    LEFT JOIN {{ ref('dim_unit') }} u
        ON wfp.um_id = u.id
    WHERE 
        wfp.mp_price IS NOT NULL
        AND wfp.mp_month IS NOT NULL
        AND wfp.mp_year IS NOT NULL
),

adjusted_transactions AS (
    SELECT
        *,
        CASE
            WHEN unit_in_kg IS NOT NULL AND unit_in_kg > 0 THEN price / unit_in_kg
            ELSE NULL
        END AS price_per_kg
    FROM base_transactions
),

transactions_with_date AS (
    SELECT
        at.*,
        d.date_id AS dim_date_id
    FROM adjusted_transactions at
    LEFT JOIN {{ ref('dim_date') }} d
    ON at.year = d.year
    AND at.month = d.month
),

transactions_with_weather AS (
    SELECT
        td.*,
        w.id AS weather_id
    FROM transactions_with_date td
    LEFT JOIN {{ ref('dim_weather') }} w
    ON td.locality_id = w.locality_id
    AND td.dim_date_id = w.dim_date_id
),
transactions_with_currency_value AS (
    SELECT
        tw.*,
        cv.id AS currency_value_id
    FROM transactions_with_weather tw
    INNER JOIN {{ ref('dim_currency_value') }} cv
    ON tw.currency_id = cv.currency_id
    WHERE tw.year = cv.year AND tw.month = cv.month AND cv.value IS NOT NULL

)

SELECT
    ROW_NUMBER() OVER () AS transaction_id, -- Auto-increment ID for fact table
    market_id as dim_market_id,
    commodity_id as dim_commodity_id,
    currency_id as dim_currency_id,
    currency_value_id,
    unit_id as dim_unit_id,
    dim_date_id,
    weather_id,
    locality_id as dim_locality_id,
    price,
    price_per_kg,
    commodity_source
FROM transactions_with_currency_value
ORDER BY transaction_id
