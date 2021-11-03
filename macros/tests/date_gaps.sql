{% macro date_gaps(
        table,
        partition_by,
        column,
        exclude ={}
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
    DATEDIFF(
        days,
        {{ previous_column }},
        {{ column }}
    ) - 1 AS gap
FROM
    source
WHERE
    DATEDIFF(
        days,
        {{ previous_column }},
        {{ column }}
    ) > 1 {% if exclude -%}
        AND {{ column }} NOT IN (
            {{ dbt_utils.date_spine(
                datepart = "day",
                start_date = exclude.start_date,
                end_date = exclude.end_date
            ) }}
        )
    {%- endif -%}
ORDER BY
    gap DESC
{% endmacro %}
