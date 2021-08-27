{{ config(
    materialized = 'incremental',
    sort = ['date', 'currency'],
    unique_key = 'date',
    tags=['snowflake', 'terra_gold', 'terra_daily_balances']
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
