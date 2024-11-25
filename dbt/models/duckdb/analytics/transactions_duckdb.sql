{{ config(materialized='table', schema='analytics') }}

SELECT * FROM postgres_scan('dbname=market_flow', 'public', 'transactions')
