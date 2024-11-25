{{ config(materialized='table') }}

select *
from {{ source('external', 'raw_hdi') }}
