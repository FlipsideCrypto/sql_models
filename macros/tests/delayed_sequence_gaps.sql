{% macro delayed_sequence_gaps(
        table,
        partition_by,
        column,
        delayed_column
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
            ) AS {{ previous_column }}
        FROM
            {{ table }}
        WHERE
            {{ delayed_column }} < (
                SELECT
                    MAX(
                        {{ delayed_column }}
                    )
                FROM
                    {{ this }}
            ) - INTERVAL '15 HOURS'
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
ORDER BY
    gap DESC
{% endmacro %}
