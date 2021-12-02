{% macro date_gaps(
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
    DATEDIFF(
        days,
        {{ previous_column }},
        {{ column }}
    ) - 1 AS gap
FROM
    source
    {% if varargs -%}
LEFT JOIN (
        {% for x in varargs %}
            (
            {{ dbt_utils.date_spine(
                datepart = "day",
                start_date = x.start_date,
                end_date = x.end_date
            ) }}
            )
            {{- "UNION ALL" if not loop.last -}}
        {% endfor %}
) exclude
    ON source.day = exclude.date_day
    {%- endif %}
WHERE
    DATEDIFF(
        days,
        {{ previous_column }},
        {{ column }}
    ) > 1 
    {{ "AND source.day != exclude.date_day" if varargs }}
ORDER BY
    gap DESC
{% endmacro %}
