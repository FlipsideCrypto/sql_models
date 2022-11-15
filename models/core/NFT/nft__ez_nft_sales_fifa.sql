{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    tx_group_id,
    purchaser,
    A.nft_asset_id,
    number_of_nfts,
    total_sales_amount_USD,
    A.type AS sale_type
FROM
    {{ ref('silver__nft_sales_fifa_collect') }} A
    JOIN {{ ref('core__dim_block') }}
    b
    ON A.block_id = b.block_id
