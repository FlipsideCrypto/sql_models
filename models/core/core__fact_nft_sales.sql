{{ config(
    materialized = 'incremental',
    unique_key = 'fact_nft_sales_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        'ab2 gallery' AS nft_marketplace,
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
        'shufl' AS nft_marketplace,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_sales_shufl') }}
    UNION ALL
    SELECT
        'atomic swaps' AS nft_marketplace,
        block_id,
        tx_group_id,
        purchaser,
        nft_asset_id,
        number_of_nfts,
        total_sales_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__nft_atomic_swaps') }}
    WHERE
        concat_ws(
            tx_group_id :: STRING,
            nft_asset_id :: STRING
        ) NOT IN (
            SELECT
                concat_ws(
                    tx_group_id :: STRING,
                    nft_asset_id :: STRING
                )
            FROM
                {{ ref('silver__nft_sales_rand_gallery') }}
        )
        AND concat_ws(
            tx_group_id :: STRING,
            nft_asset_id :: STRING
        ) NOT IN (
            SELECT
                concat_ws(
                    tx_group_id :: STRING,
                    nft_asset_id :: STRING
                )
            FROM
                {{ ref('silver__nft_sales_algoxnft') }}
        )
),
mid AS (
    SELECT
        *
    FROM
        base

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
    OR block_id || '--' || tx_group_id || '--' || nft_asset_id IN (
        SELECT
            block_id || '--' || tx_group_id || '--' || nft_asset_id
        FROM
            {{ this }}
        WHERE
            dim_account_id__purchaser = '-1'
            OR dim_asset_id__nft = '-1'
            OR dim_block_id = '-1'
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.tx_group_id','a.nft_asset_id']
    ) }} AS fact_nft_sales_id,
    nft_marketplace,
    COALESCE(
        d.block_timestamp,
        '1900-01-01' :: DATE
    ) block_timestamp,
    COALESCE(
        d.dim_block_id,
        '-1'
    ) AS dim_block_id,
    tx_group_id,
    purchaser,
    COALESCE(
        b.dim_account_id,
        '-1'
    ) AS dim_account_id__purchaser,
    nft_asset_id,
    COALESCE(
        C.dim_asset_id,
        '-1'
    ) AS dim_asset_id__nft,
    number_of_nfts,
    total_sales_amount,
    A._INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_account') }}
    b
    ON A.purchaser = b.address
    LEFT JOIN {{ ref('core__dim_asset') }} C
    ON A.nft_asset_id = C.asset_id
    LEFT JOIN {{ ref('core__dim_block') }}
    d
    ON A.block_id = d.block_id
