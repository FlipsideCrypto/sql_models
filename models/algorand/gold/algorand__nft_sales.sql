{{ config(
    materialized = 'view'
) }}

SELECT
    'ab2 gallery' AS nft_marketplace,
    block_timestamp,
    block_id,
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount
FROM
    {{ ref('silver_algorand__nft_sales_ab2_gallery') }}
UNION ALL
SELECT
    'algoxnft' AS nft_marketplace,
    block_timestamp,
    block_id,
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount
FROM
    {{ ref('silver_algorand__nft_sales_algoxnft') }}
UNION ALL
SELECT
    'octorand' AS nft_marketplace,
    block_timestamp,
    block_id,
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount
FROM
    {{ ref('silver_algorand__nft_sales_octorand') }}
UNION ALL
SELECT
    'rand gallery' AS nft_marketplace,
    block_timestamp,
    block_id,
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount
FROM
    {{ ref('silver_algorand__nft_sales_rand_gallery') }}
UNION ALL
SELECT
    'atomic swaps' AS nft_marketplace,
    block_timestamp,
    block_id,
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount
FROM
    {{ ref('silver_algorand__nft_atomic_swaps') }}
