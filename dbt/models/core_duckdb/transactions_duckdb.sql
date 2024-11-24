{{ config(materialized='table', schema='analytics') }}

SELECT *
FROM {{ postgres_scan('public', 'transactions') }}
