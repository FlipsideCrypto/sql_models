{{ config(
    materialized = 'incremental',
    unique_key = 'asset_id',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'asset', 'silver_algorand']
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
),
collection_NFTs AS (
    SELECT
        DISTINCT VALUE :asset :: NUMBER AS nft,
        VALUE :collection :: STRING AS collection
    FROM
        collect_NFTs,
        LATERAL FLATTEN(
            input => record_content
        ) f
    WHERE
        VALUE :asset :: STRING <> ''
),
arc69_NFTs AS(
    SELECT
        DISTINCT asset_id AS nft
    FROM
        {{ ref('silver_algorand__asset_configuration_transaction') }}
    WHERE
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                tx_message :txn :note :: STRING
            )
        ) :standard :: STRING = 'arc69'
),
arc3_NFTs AS (
    SELECT
        DISTINCT asset_id AS nft
    FROM
        {{ ref('silver_algorand__asset') }}
    WHERE
        asset_url LIKE '%#arc3%'
),
traditional_NFTs AS(
    SELECT
        DISTINCT asset_id AS nft
    FROM
        {{ ref('silver_algorand__asset') }}
    WHERE
        decimals = 0
        AND total_supply = 1
),
all_NFTs AS (
    SELECT
        nft
    FROM
        collection_NFTs
    UNION
    SELECT
        nft
    FROM
        arc69_NFTs
    UNION
    SELECT
        nft
    FROM
        arc3_NFTs
    UNION
    SELECT
        nft
    FROM
        traditional_NFTs
)
SELECT
    DISTINCT allNFT.nft AS asset_id,
    asset.asset_name AS asset_name,
    asset.total_supply AS total_supply,
    asset.decimals AS decimals,
    asset.created_at AS created_at,
    coll.collection AS collection_name,
    asset.creator_address AS creator_address,
    asset.asset_url AS asset_url,
    CASE
        WHEN coll.nft IS NOT NULL THEN 'TRUE'
        ELSE 'FALSE'
    END AS collection_NFT,
    CASE
        WHEN arc69.nft IS NOT NULL THEN 'TRUE'
        ELSE 'FALSE'
    END AS arc69_NFT,
    CASE
        WHEN arc3.nft IS NOT NULL THEN 'TRUE'
        ELSE 'FALSE'
    END AS ar3_NFT,
    CASE
        WHEN trad.nft IS NOT NULL THEN 'TRUE'
        ELSE 'FALSE'
    END AS traditional_NFT,
    asset.asset_deleted AS asset_deleted
FROM
    all_NFTs allNFT
    LEFT JOIN collection_NFTs coll
    ON allNFT.nft = coll.nft
    LEFT JOIN arc69_NFTs arc69
    ON allNFT.nft = arc69.nft
    LEFT JOIN arc3_NFTs arc3
    ON allNFT.nft = arc3.nft
    LEFT JOIN traditional_NFTs trad
    ON allNFT.nft = trad.nft
    LEFT JOIN {{ ref('silver_algorand__asset') }}
    asset
    ON allNFT.nft = asset.asset_id
