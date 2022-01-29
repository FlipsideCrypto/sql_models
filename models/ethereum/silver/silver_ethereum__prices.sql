{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', token_address, symbol, HOUR)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['HOUR'],
    tags = ['snowflake', 'ethereum', 'silver_ethereum__prices']
) }}

WITH decimals AS (

    SELECT
        LOWER(address) AS address,
        meta :decimals :: INT AS decimals
    FROM
        {{ ref('silver_ethereum__contracts') }}
    WHERE
        meta :decimals NOT LIKE '%00%' qualify(ROW_NUMBER() over(PARTITION BY contract_address, NAME, symbol --need the %00% filter to exclude messy data
    ORDER BY
        decimals DESC) = 1)
),
metadata AS (
    WITH tmp AS (
        SELECT
            token_address,
            symbol,
            COUNT(1)
        FROM
            {{ source(
                'shared',
                'market_asset_metadata'
            ) }}
        WHERE
            platform = 'ethereum'
        GROUP BY
            token_address,
            symbol
    ),
    tmp2 AS (
        SELECT
            token_address,
            COUNT(1)
        FROM
            tmp
        GROUP BY
            token_address
        HAVING
            COUNT(1) > 1
    ),
    tmp3 AS (
        SELECT
            token_address,
            ARRAY_AGG(
                DISTINCT symbol
            ) within GROUP (
                ORDER BY
                    symbol ASC
            ) AS symbol_array
        FROM
            {{ source(
                'shared',
                'market_asset_metadata'
            ) }}
        WHERE
            token_address IN (
                SELECT
                    token_address
                FROM
                    tmp2
            )
        GROUP BY
            token_address
    ),
    tmp4 AS (
        SELECT
            token_address,
            ARRAY_TO_STRING(
                symbol_array,
                ','
            ) AS symbol
        FROM
            tmp3
    )
    SELECT
        m.asset_id,
        m.token_address,
        COALESCE(
            t.symbol,
            m.symbol
        ) AS symbol
    FROM
        {{ source(
            'shared',
            'market_asset_metadata'
        ) }}
        m
        LEFT JOIN tmp4 t
        ON m.token_address = t.token_address
    WHERE
        platform = 'ethereum'
        AND m.token_address IS NOT NULL
    GROUP BY
        m.asset_id,
        m.token_address,
        COALESCE(
            t.symbol,
            m.symbol
        )
    ORDER BY
        token_address DESC
),
coinmarketcap AS (
    SELECT
        m.symbol,
        LOWER(
            m.token_address
        ) AS token_address,
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS HOUR,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
        prices
        JOIN metadata m
        ON prices.asset_id = m.asset_id
    WHERE
        provider = 'coinmarketcap'

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '2 days'
{% endif %}
GROUP BY
    m.symbol,
    LOWER(
        m.token_address
    ),
    HOUR
),
coingecko AS (
    SELECT
        m.symbol,
        LOWER(
            m.token_address
        ) AS token_address,
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS HOUR,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
        prices
        JOIN metadata m
        ON prices.asset_id = m.asset_id
    WHERE
        provider = 'coingecko'

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '2 days'
{% endif %}
GROUP BY
    m.symbol,
    LOWER(
        m.token_address
    ),
    HOUR
),
decimals_old AS (
    SELECT
        LOWER(address) AS address,
        decimals
    FROM
        {{ source(
            'ethereum',
            'ethereum_contract_decimal_adjustments'
        ) }}
    WHERE
        decimals IS NOT NULL
)
SELECT
    COALESCE(
        cg.symbol,
        cmc.symbol
    ) AS symbol,
    COALESCE(
        cg.hour,
        cmc.hour
    ) AS HOUR,
    COALESCE(
        cg.token_address,
        cmc.token_address
    ) AS token_address,
    COALESCE(
        cg.price,
        cmc.price
    ) AS price,
    COALESCE(
        d.decimals,
        DO.decimals
    ) AS decimals
FROM
    coingecko cg full
    OUTER JOIN coinmarketcap cmc
    ON DATE_TRUNC(
        'hour',
        cg.hour
    ) = DATE_TRUNC(
        'hour',
        cmc.hour
    )
    AND LOWER(
        cg.token_address
    ) = LOWER(
        cmc.token_address
    )
    LEFT JOIN decimals d
    ON d.address = LOWER(
        COALESCE(
            cg.token_address,
            cmc.token_address
        )
    )
    LEFT JOIN decimals_old DO
    ON DO.address = LOWER(
        COALESCE(
            cg.token_address,
            cmc.token_address
        )
    )
