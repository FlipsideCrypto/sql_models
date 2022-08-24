{{ config(
    materialized = 'view'
) }}

SELECT
    asset_id AS nft_asset_id,
    asset_name AS nft_asset_name,
    total_supply AS nft_total_supply,
    decimals,
    created_at,
    collection_name,
    creator_address,
    asset_deleted,
    asset_url AS nft_url,
    collection_nft,
    arc69_nft,
    ar3_nft,
    ar19_nft,
    traditional_nft
FROM
    {{ ref('core__dim_asset') }}
WHERE
    is_nft = TRUE
