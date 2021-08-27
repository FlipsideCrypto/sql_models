{{ config(
    materialized = 'table'
) }}

SELECT
    *
FROM
    {{ source(
        'shared',
        'udm_decimal_adjustments'
    ) }}
WHERE
    blockchain = 'terra'
