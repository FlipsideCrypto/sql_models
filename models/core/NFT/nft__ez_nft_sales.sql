{{ config(
    materialized = 'view'
) }}

SELECT
    nft_marketplace,
    block_timestamp,
    tx_group_id,
    purchaser,
    C.created_at AS purchaser_account_created_at,
    A.nft_asset_id,
    nft_asset_name,
    nft_total_supply,
    nft_url,
    collection_name,
    number_of_nfts,
    total_sales_amount
FROM
    {{ ref('nft__fact_nft_sales') }} A
    JOIN {{ ref('nft__ez_nft_asset') }}
    b
    ON A.nft_asset_id = b.nft_asset_id
    JOIN {{ ref('core__dim_account') }} C
    ON A.dim_account_id__purchaser = C.dim_account_id
