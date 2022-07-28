{{ config(
    materialized = 'view'
) }}

SELECT
    nft_asset_id,
    nft_asset_name,
    nft_total_supply,
    decimals,
    created_at,
    collection_name,
    creator_address,
    nft_url,
    collection_nft,
    arc69_nft,
    ar3_nft,
    traditional_nft,
    asset_deleted
FROM
    {{ ref('silver_algorand__nft_asset') }}
