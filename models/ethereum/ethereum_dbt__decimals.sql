{{ config(
    materialized = 'table',
    unique_key = 'token_id',
    tags = ['snowflake', 'ethereum', 'ethereum_dbt_decimals']
) }}

SELECT
    DISTINCT token_address AS token_id,
    decimals :: INT AS decimals
FROM
    {{ ref('ethereum__token_prices_hourly') }}
WHERE
    decimals IS NOT NULL
