{{ config(
    materialized = 'view',
    secure = true,
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'new_eth']
) }}

SELECT
    *
FROM
    {{ ref('ethereum__dex_liquidity_pools') }}
