{{ config(
    materialized = 'incremental',
    unique_key = "asset_id",
    incremental_strategy = 'merge'
) }}

WITH cmc_base AS (

    SELECT
        COALESCE(
            VALUE :contract_address :: STRING,
            '0'
        ) AS asset_id,
        NAME AS cmc_name,
        cmc_id,
        symbol AS cmc_symbol,
        metadata :description :: STRING AS cmc_decription,
        metadata :logo :: STRING AS cmc_icon,
        metadata :tags AS cmc_tags,
        metadata :explorer :: STRING AS cmc_explorer,
        metadata :twitter AS cmc_twitter,
        metadata :website AS cmc_urls,
        _inserted_timestamp
    FROM
        {{ source(
            'crosschain_silver',
            'coin_market_cap_cryptocurrency_info'
        ) }},
        LATERAL FLATTEN(
            metadata :contract_address,
            outer => TRUE
        )
    WHERE
        (
            (
                VALUE :platform :name = 'Algorand'
                AND TRY_CAST(
                    VALUE :contract_address :: STRING AS INT
                ) IS NOT NULL
            )
            OR symbol = 'ALGO'
        )
),
base AS (
    SELECT
        DISTINCT COALESCE(
            token_address,
            '0'
        ) AS asset_id
    FROM
        {{ source(
            'crosschain_silver',
            'asset_metadata_coin_gecko'
        ) }}
    WHERE
        (
            (
                platform = 'algorand'
                AND TRY_CAST(
                    token_address AS INT
                ) IS NOT NULL
            )
            OR symbol = 'algo'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    DISTINCT asset_id
FROM
    cmc_base

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
fin AS (
    SELECT
        A.asset_id,
        cg.id AS coin_gecko_id,
        cmc.cmc_id AS coin_market_cap_id,
        cg.name AS cg_name,
        cg.symbol AS cg_symbol,
        cmc.cmc_name,
        cmc.cmc_symbol,
        cmc.cmc_decription,
        cmc.cmc_icon,
        cmc.cmc_tags,
        cmc.cmc_twitter,
        cmc.cmc_urls,
        GREATEST(
            COALESCE(
                cg._inserted_timestamp,
                '1900-01-01'
            ),
            COALESCE(
                cmc._inserted_timestamp,
                '1900-01-01'
            )
        ) AS _inserted_timestamp
    FROM
        base A
        LEFT JOIN (
            SELECT
                id,
                COALESCE(
                    token_address,
                    '0'
                ) AS asset_id,
                NAME,
                symbol,
                _inserted_timestamp
            FROM
                {{ source(
                    'crosschain_silver',
                    'asset_metadata_coin_gecko'
                ) }}
            WHERE
                (
                    (
                        platform = 'algorand'
                        AND TRY_CAST(
                            token_address AS INT
                        ) IS NOT NULL
                    )
                    OR symbol = 'algo'
                ) qualify(ROW_NUMBER() over(PARTITION BY asset_id
            ORDER BY
                _inserted_timestamp DESC) = 1)
        ) cg
        ON A.asset_id = cg.asset_id
        LEFT JOIN cmc_base cmc
        ON A.asset_id = cmc.asset_id
)
SELECT
    asset_id,
    COALESCE(
        cg_name,
        cmc_name
    ) AS token_name,
    COALESCE(
        cg_symbol,
        cmc_symbol
    ) AS symbol,
    coin_gecko_id,
    coin_market_cap_id,
    NULLIF(
        cmc_tags,
        'null'
    ) AS tags,
    cmc_icon AS logo,
    cmc_twitter AS twitter,
    cmc_urls AS website,
    cmc_decription AS decription,
    _inserted_timestamp
FROM
    fin
