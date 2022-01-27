{{ config(
    materialized = "incremental",
    cluster_by = "recorded_at::DATE",
    unique_key = "_unique_key",
    incremental_strategy = "merge",
    tags = ["snowflake", "market", "stats", "gold", "fact", "f_stats_usd"]
) }}

SELECT
    COALESCE(
        asset_id,
        '#UNKNOWN'
    ) AS asset_id,
    provider,
    NAME,
    symbol,
    recorded_at,
    circulating_supply,
    market_cap,
    price,
    max_supply,
    total_supply,
    volume_24h,
    SYSDATE() AS _updated,
    concat_ws(
        '-',
        NAME,
        symbol,
        provider,
        to_varchar(
            recorded_at,
            'YYYY-MM-DD HH24:MI:SS.FF9'
        )
    ) AS _unique_key
FROM
    {{ source(
        "shared",
        "prices_v2"
    ) }}
WHERE
    NAME IS NOT NULL
    AND symbol IS NOT NULL
    AND provider IS NOT NULL
    AND recorded_at IS NOT NULL

{% if is_incremental() %}
AND recorded_at >= (
    SELECT
        MAX(recorded_at)
    FROM
        {{ this }}
) - INTERVAL '30 DAYS'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY NAME,
    symbol,
    provider,
    recorded_at
    ORDER BY
        to_varchar(
            recorded_at,
            'YYYY-MM-DD HH24:MI:SS.FF9'
        )
) = 1
