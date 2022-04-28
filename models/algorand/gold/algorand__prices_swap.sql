{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'algorand_swaps', 'algorand_prices', 'gold'],
) }}

SELECT
    block_hour,
    asset_id,
    asset_name,
    price_usd,
    min_price_usd_hour,
    max_price_usd_hour,
    volatility_measure,
    swaps_in_hour,
    volume_usd_in_hour
FROM
    {{ ref('silver_algorand__prices_swap') }}
