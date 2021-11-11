{% macro all_ids_exist(
        base_table,
        base_id,
        other_table,
        other_id
    ) %}
    WITH base AS (
        SELECT
            DISTINCT {{ base_id }} AS id
        FROM
            {{ base_table }}
    ), other AS (
        SELECT
            DISTINCT {{ other_id }} AS id
        FROM
            {{ other_table }}
    )
SELECT b.*
FROM
    base b
LEFT JOIN 
    other o 
ON o.id = b.id
WHERE o.id IS NULL
{% endmacro %}
