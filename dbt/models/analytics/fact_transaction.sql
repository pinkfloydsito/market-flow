{{ config(materialized='table') }}

WITH base_transactions AS (
    SELECT
        transactions.market_id,
        transactions.commodity_id,
        transactions.currency_id,
        transactions.unit_id,
        transactions.locality_id,
        transactions.year,
        transactions.month,
        transactions.price,
        transactions.price_per_kg
    FROM {{ ref('stg_transactions') }} transactions
    INNER JOIN {{ ref('dim_market') }} mkt
        ON transactions.market_id = mkt.id
    INNER JOIN {{ ref('dim_locality') }} loc
        ON mkt.locality_id = loc.id
    LEFT JOIN {{ ref('dim_unit') }} u
        ON transactions.unit_id = u.id
    WHERE 
        transactions.price IS NOT NULL
        AND transactions.month IS NOT NULL
        AND transactions.year IS NOT NULL
),

transactions_with_date AS (
    SELECT
        at.*,
        d.id AS dim_date_id
    FROM base_transactions at
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
transactions_with_currency_historical AS (
    SELECT
        tw.*,
        cv.id AS currency_historical_id
    FROM transactions_with_weather tw
    INNER JOIN {{ ref('dim_currency_historical') }} cv
    ON tw.currency_id = cv.currency_id
    WHERE tw.year = cv.year AND tw.month = cv.month AND cv.value IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER () AS transaction_id,
    market_id as dim_market_id,
    commodity_id as dim_commodity_id,
    currency_id as dim_currency_id,
    currency_historical_id as dim_currency_historical_id,
    unit_id as dim_unit_id,
    dim_date_id,
    weather_id as dim_weather_id,
    locality_id as dim_locality_id,
    price,
    price_per_kg
FROM transactions_with_currency_historical
ORDER BY transaction_id
