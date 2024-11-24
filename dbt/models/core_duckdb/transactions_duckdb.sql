{{ config(materialized='table', schema='analytics') }}

--  SELECT * FROM postgres_scan('public', 'transactions')
{{ config(materialized='table', schema='analytics') }}

SELECT *
FROM postgres_scan(
    schema_name => 'public',
    table_name => 'transactions',
    host => '{{ var("postgres_host", "postgres") }}',
    port => {{ var("postgres_port", 5432) }},
    user => '{{ var("postgres_user", "airflow") }}',
    password => '{{ var("postgres_password", "airflow") }}',
    database => '{{ var("postgres_database", "market_flow") }}'
)

-- select * from {{ ref('transactions') }}
