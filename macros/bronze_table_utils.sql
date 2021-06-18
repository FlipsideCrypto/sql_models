{% macro bronze_kafka_extract(
    bronze_src_tables, 
    columns, 
    dedupe_key, 
    timestamp_col = "block_timestamp", 
    kafka_incremental_window = '2 days',
    table_incremental_window = '1 days'
) -%}

    WITH model_results as (
    {% for table_name in bronze_src_tables %}
        {% if loop.index > 1 and bronze_src_tables|length > 1 %}
        UNION ALL
        {% endif %}

        SELECT
        {{ columns }},
        to_timestamp(((record_metadata:CreateTime::double/1000)::bigint)) as system_created_at
        FROM
        {{table_name}},
        TABLE(FLATTEN({{table_name}}.record_content:results)) t

        {% if is_incremental() %}
        WHERE 
        record_metadata:CreateTime >= DATE_PART('EPOCH_MILLISECOND', GETDATE() - interval '{{kafka_incremental_window}}')
        {% endif %}

    {% endfor %}
    )

    SELECT
    *
    FROM
    (
    SELECT 
        *,
        row_number() OVER (PARTITION BY {{dedupe_key}} ORDER BY system_created_at DESC) AS system_row_number
    FROM model_results
    ) sq
    WHERE system_row_number = 1
    {% if is_incremental() %}
    AND {{timestamp_col}} >= getdate() - interval '{{table_incremental_window}}'
    {% endif %}

{%- endmacro %}