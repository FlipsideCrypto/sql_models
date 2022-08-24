{% test negative_one(
    model,
    column_name
) %}
SELECT
    *
FROM
    {{ model }}
WHERE
    {{ column_name }} = '-1' {% endtest %}
