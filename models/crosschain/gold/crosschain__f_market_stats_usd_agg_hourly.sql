{{ config(
    materialized = "incremental",
    unique_key = "_unique_key",
    incremental_strategy = "merge",
    tags = ["crosschain", "gold", "fact", "f_stats_usd", "aggregate", "daily"]
) }}

SELECT
    asset_id,
    provider,
    platform,
    DATE_TRUNC(
        HOUR,
        recorded_at
    ) AS recorded_at,
    COUNT(*) AS n,
    AVG(circulating_supply) AS avg_circulating_supply,
    AVG(market_cap) AS avg_market_cap,
    AVG(max_supply) AS avg_max_supply,
    AVG(price) AS avg_price,
    AVG(total_supply) AS avg_total_supply,
    AVG(volume_24h) AS avg_volume_24h,
    MAX(circulating_supply) AS max_circulating_supply,
    MAX(market_cap) AS max_market_cap,
    MAX(max_supply) AS max_max_supply,
    MAX(price) AS max_price,
    MAX(total_supply) AS max_total_supply,
    MAX(volume_24h) AS max_volume_24h,
    MEDIAN(circulating_supply) AS median_circulating_supply,
    MEDIAN(market_cap) AS median_market_cap,
    MEDIAN(max_supply) AS median_max_supply,
    MEDIAN(price) AS median_price,
    MEDIAN(total_supply) AS median_total_supply,
    MEDIAN(volume_24h) AS median_volume_24h,
    MIN(circulating_supply) AS min_circulating_supply,
    MIN(market_cap) AS min_market_cap,
    MIN(max_supply) AS min_max_supply,
    MIN(price) AS min_price,
    MIN(total_supply) AS min_total_supply,
    MIN(volume_24h) AS min_volume_24h,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'circulating_supply',
            circulating_supply,
            'market_cap',
            market_cap,
            'max_supply',
            max_supply,
            'price',
            price,
            'total_supply',
            total_supply,
            'volume_24h',
            volume_24h,
            'recorded_at',
            recorded_at
        )
    ) within GROUP (
        ORDER BY
            recorded_at
    ) AS DATA,
    DATA [0] :market_cap AS first_market_cap,
    DATA [0] :max_supply AS first_max_supply,
    DATA [0] :price AS first_price,
    DATA [0] :total_supply AS first_total_supply,
    DATA [0] :volume_24h AS first_volume_24h,
    DATA [ARRAY_SIZE(data)-1] :market_cap AS last_market_cap,
    DATA [ARRAY_SIZE(data)-1] :max_supply AS last_max_supply,
    DATA [ARRAY_SIZE(data)-1] :price AS last_price,
    DATA [ARRAY_SIZE(data)-1] :total_supply AS last_total_supply,
    DATA [ARRAY_SIZE(data)-1] :volume_24h AS last_volume_24h,
    SYSDATE() AS _updated_timestamp,
    concat_ws(
        '-',
        asset_id,
        NAME,
        symbol,
        provider,
        DATE_TRUNC(
            HOUR,
            recorded_at
        )
    ) AS _unique_key
FROM
    {{ ref(
        "silver_crosschain__f_market_stats_usd"
    ) }}

{% if is_incremental() %}
WHERE
    recorded_at >= DATE_TRUNC(
        HOUR,(
            SELECT
                MAX(recorded_at)
            FROM
                {{ this }}
        )
    ) - INTERVAL '1 hour'
{% endif %}
GROUP BY
    asset_id,
    provider,
    platform,
    DATE_TRUNC(
        HOUR,
        recorded_at
    )
