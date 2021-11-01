{% macro sequence_gaps(
        table,
        partition_by,
        column
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
