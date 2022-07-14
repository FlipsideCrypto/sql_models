{{ config(
    materialized = 'view'
) }}

SELECT
    block_hour,
    asset_id,
    asset_name,
    price_usd,
    algo_balance,
    non_algo_balance,
    pool_name,
    pool_address
FROM
    {{ ref('silver_algorand__prices_pool_balances') }}
