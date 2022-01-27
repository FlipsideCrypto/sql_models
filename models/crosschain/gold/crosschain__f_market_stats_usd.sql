{{ config(
    materialized = "view",
    unique_key = "_unique_key",
    tags = ["crosschain", "gold", "fact", "f_stats_usd"]
) }}

SELECT
    asset_id,
    provider,
    platform,
    recorded_at,
    circulating_supply,
    market_cap,
    price,
    max_supply,
    total_supply,
    volume_24h,
    _updated,
    _unique_key
FROM
    {{ ref(
        "silver_crosschain__f_market_stats_usd"
    ) }}
