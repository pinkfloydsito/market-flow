{% macro postgres_scan(schema_name, table_name) %}
    postgres_scan(
        schema_name => '{{ schema_name }}',
        table_name => '{{ table_name }}',
        host => 'postgres',
        port => 5432,
        user => 'airflow',
        password => 'airflow',
        database => 'market_flow'
    )
{% endmacro %}
