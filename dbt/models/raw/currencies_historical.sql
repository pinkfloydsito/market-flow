{{ config(materialized='table') }}

select * from {{ source('raw', 'currencies_historical') }}
