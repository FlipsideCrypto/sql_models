{{ config(
    materialized = 'incremental',
    unique_key = 'fact_nft_sales_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        'ab2 gallery' AS nft_marketplace,
        block_timestamp,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_sales_ab2_gallery') }}
    UNION ALL
    SELECT
        'algoxnft' AS nft_marketplace,
        block_timestamp,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_sales_algoxnft') }}
    UNION ALL
    SELECT
        'octorand' AS nft_marketplace,
        block_timestamp,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_sales_octorand') }}
    UNION ALL
    SELECT
        'rand gallery' AS nft_marketplace,
        block_timestamp,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_sales_rand_gallery') }}
    UNION ALL
    SELECT
        'atomic swaps' AS nft_marketplace,
        block_timestamp,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_atomic_swaps') }}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.tx_group_id','a.nft_asset_id']
    ) }} AS fact_nft_sales_id,
    nft_marketplace,
    A.block_timestamp,
    A.block_id,
    d.dim_block_id,
    tx_group_id,
    purchaser,
    b.dim_account_id AS dim_account_id__purchaser,
    nft_asset_id,
    C.dim_asset_id AS dim_asset_id__nft,
    number_of_nfts,
    total_sales_amount,
    A._INSERTED_TIMESTAMP
FROM
    base A
    JOIN {{ ref('core__dim_account') }}
    b
    ON A.purchaser = b.address
    JOIN {{ ref('core__dim_asset') }} C
    ON A.nft_asset_id = C.asset_id
    JOIN {{ ref('core__dim_block') }}
    d
    ON A.block_id = d.block_id
