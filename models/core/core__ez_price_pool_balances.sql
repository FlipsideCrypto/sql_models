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
    pool_address,
    _algo_price
FROM
    {{ ref('silver__price_pool_balances') }}
