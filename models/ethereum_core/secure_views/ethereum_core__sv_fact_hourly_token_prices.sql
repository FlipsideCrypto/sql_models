{{ config(
    materialized = 'view',
    secure = true,
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'new_eth']
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ ref('ethereum__token_prices_hourly') }}
