{{ config(
    materialized = 'view'
) }}

SELECT
    block_hour,
    A.asset_id,
    asset_name,
    price_usd,
    min_price_usd_hour,
    max_price_usd_hour,
    volatility_measure,
    swaps_in_hour,
    volume_usd_in_hour
FROM
    {{ ref('silver__price_swap') }} A
    JOIN {{ ref('core__dim_asset') }}
    b
    ON A.asset_id = b.asset_id
