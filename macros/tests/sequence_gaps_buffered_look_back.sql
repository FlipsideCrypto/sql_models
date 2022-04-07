{% macro sequence_gaps_buffered_look_back(
        table,
        partition_by,
        column,
        delayed_column,
        delayed_period
    ) %}
    {%- set partition_sql = partition_by | join(", ") -%}
    {%- set previous_column = "prev_" ~ column -%}
    WITH source AS (
        SELECT
            {{ partition_sql + "," if partition_sql }}
            {{ column }},
            LAG(
                {{ column }},
                1
            ) over (
                {{ "PARTITION BY " ~ partition_sql if partition_sql }}
                ORDER BY
                    {{ column }} ASC
            ) AS {{ previous_column }},
            LAG(
                {{ delayed_column }},
                1
            ) over (
                {{ "PARTITION BY " ~ partition_sql if partition_sql }}
                ORDER BY
                    {{ column }} ASC
            ) AS {{ delayed_column }}
        FROM
            {{ table }}
    )
SELECT
    {{ partition_sql + "," if partition_sql }}
    {{ previous_column }},
    {{ column }},
    {{ column }} - {{ previous_column }}
    - 1 AS gap
FROM
    source
WHERE
    {{ column }} - {{ previous_column }} <> 1
AND 
    {{ delayed_column }} < (
        SELECT
            MAX(
                {{ delayed_column }}
            )
        FROM
            {{ this }}
    ) - INTERVAL '{{ delayed_period }}'
{% endmacro %}
