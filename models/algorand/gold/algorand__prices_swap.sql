{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'algorand_swaps', 'algorand_prices', 'gold'],
) }}

SELECT
    block_hour,
    asset_id,
    asset_name,
    price,
    min_price_usd_hour,
    max_price_usd_hour,
    volatility_measure,
    swaps_in_hour,
    volume_in_hour,
    price_source
FROM
    {{ ref('silver_algorand__prices_swap') }}
