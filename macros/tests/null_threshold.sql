{% macro null_threshold(
        table,
        column,
        threshold_percent
    ) %}
    -- threshold_percent: decimal representing percent of values that should NOT be null
    WITH t AS (
        SELECT
            COUNT(*) * {{ threshold_percent }} AS threshold_num
        FROM
            {{ table }}
    ),
    C AS (
        SELECT
            COUNT(*) AS cnt
        FROM
            {{ table }}
        WHERE
            {{ column }} IS NOT NULL
    )
SELECT
    *
FROM
    C
WHERE
    C.cnt <= (
        SELECT
            MAX(threshold_num)
        FROM
            t
    )
{% endmacro %}
