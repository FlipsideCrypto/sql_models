{{ config(
    materialized = 'view'
) }}

SELECT
    b.date_hour AS recorded_hour,
    A.asset_id,
    da.asset_name,
    A.symbol,
    CASE
        WHEN cg.imputed = FALSE THEN cg.close
        WHEN cmc.imputed = FALSE THEN cmc.close
        WHEN cg.imputed = TRUE THEN cg.close
        WHEN cmc.imputed = TRUE THEN cmc.close
    END AS CLOSE,
    CASE
        WHEN cg.imputed = FALSE THEN cg.imputed
        WHEN cmc.imputed = FALSE THEN cmc.imputed
        WHEN cg.imputed = TRUE THEN cg.imputed
        WHEN cmc.imputed = TRUE THEN cmc.imputed
    END AS is_imputed
FROM
    {{ ref('silver__token_metadata') }} A
    JOIN {{ ref('core__dim_asset') }}
    da
    ON A.asset_id = da.asset_id
    CROSS JOIN {{ source(
        'crosschain',
        'dim_date_hours'
    ) }}
    b
    LEFT JOIN {{ ref('silver__token_prices_coin_gecko_hourly') }}
    cg
    ON A.coin_gecko_id = cg.id
    AND b.date_hour = cg.recorded_hour
    LEFT JOIN {{ ref('silver__token_prices_coin_market_cap_hourly') }}
    cmc
    ON A.coin_market_cap_id = cmc.id
    AND b.date_hour = cmc.recorded_hour
WHERE
    COALESCE(
        cg.imputed,
        cmc.imputed
    ) IS NOT NULL
