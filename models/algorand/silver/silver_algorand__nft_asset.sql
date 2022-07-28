{{ config(
    materialized = 'incremental',
    unique_key = 'nft_asset_id',
    incremental_strategy = 'merge'
) }}

WITH collect_NFTs AS(

    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'prod_nft_metadata_uploads_1828572827'
        ) }}
    WHERE
        record_metadata :key LIKE '%algo-nft-meta%'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
collection_NFTs AS (
    SELECT
        DISTINCT VALUE :asset :: NUMBER AS nft,
        VALUE :collection :: STRING AS collection,
        VALUE :manager :: STRING AS manager,
        VALUE :name :: STRING AS NAME,
        VALUE :url :: STRING AS url,
        _INSERTED_TIMESTAMP
    FROM
        collect_NFTs,
        LATERAL FLATTEN(
            input => record_content
        ) f
    WHERE
        VALUE :asset :: STRING <> '' qualify(ROW_NUMBER() over(PARTITION BY nft
    ORDER BY
        INDEX)) = 1
),
arc69_NFTs AS(
    SELECT
        asset_id AS nft,
        MAX(_INSERTED_TIMESTAMP) _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__asset_configuration_transaction') }}
    WHERE
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                tx_message :txn :note :: STRING
            )
        ) :standard :: STRING = 'arc69'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    asset_id
),
arc3_NFTs AS (
    SELECT
        asset_id AS nft,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__asset') }}
    WHERE
        asset_url LIKE '%#arc3%'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
traditional_NFTs AS (
    SELECT
        asset_id AS nft,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__asset') }}
    WHERE
        decimals = 0
        AND total_supply = 1

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
all_NFTs AS (
    SELECT
        nft,
        _INSERTED_TIMESTAMP
    FROM
        collection_NFTs
    UNION ALL
    SELECT
        nft,
        _INSERTED_TIMESTAMP
    FROM
        arc69_NFTs
    UNION ALL
    SELECT
        nft,
        _INSERTED_TIMESTAMP
    FROM
        arc3_NFTs
    UNION ALL
    SELECT
        nft,
        _INSERTED_TIMESTAMP
    FROM
        traditional_NFTs
),
final_nfts AS (
    SELECT
        nft,
        MAX(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        all_NFTs
    GROUP BY
        nft
)
SELECT
    allNFT.nft AS nft_asset_id,
    COALESCE(
        asset.asset_name,
        coll.name
    ) AS nft_asset_name,
    asset.total_supply AS nft_total_supply,
    asset.decimals AS decimals,
    blk.block_timestamp AS created_at,
    coll.collection AS collection_name,
    asset.creator_address AS creator_address,
    COALESCE(
        asset.asset_url,
        coll.url
    ) AS nft_url,
    CASE
        WHEN coll.nft IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS collection_nft,
    CASE
        WHEN arc69.nft IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS arc69_nft,
    CASE
        WHEN arc3.nft IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS ar3_nft,
    CASE
        WHEN trad.nft IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS traditional_nft,
    asset.asset_deleted AS asset_deleted,
    allNFT._INSERTED_TIMESTAMP
FROM
    final_nfts allNFT
    LEFT JOIN collection_NFTs coll
    ON allNFT.nft = coll.nft
    LEFT JOIN arc69_NFTs arc69
    ON allNFT.nft = arc69.nft
    LEFT JOIN arc3_NFTs arc3
    ON allNFT.nft = arc3.nft
    LEFT JOIN traditional_NFTs trad
    ON allNFT.nft = trad.nft
    JOIN {{ ref('silver_algorand__asset') }}
    asset
    ON allNFT.nft = asset.asset_id
    JOIN {{ ref('silver_algorand__block') }}
    blk
    ON asset.created_at = blk.block_id
