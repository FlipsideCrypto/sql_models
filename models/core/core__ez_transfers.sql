{{ config(
    materialized = 'view',
) }}

SELECT
    block_timestamp,
    block_timestamp :: DATE block_date,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    tx_sender,
    tx_sender AS asset_sender,
    receiver,
    0 AS asset_id,
    IFNULL(
        amount,
        0
    ) AS amount,
    asset_name,
    decimals,
    IFNULL(
        fee,
        0
    ) AS fee,
    'pay' AS tx_type,
    'payment' AS tx_type_name,
    tx_message,
    extra
FROM
    {{ ref('core__fact_transaction') }} A
    JOIN {{ ref('core__dim_asset') }}
    asset
    ON A.dim_asset_id = asset.dim_asset_id
WHERE
    dim_transaction_type_id = 'b02a45a596bfb86fe2578bde75ff5444'
UNION ALL
SELECT
    block_timestamp,
    block_timestamp :: DATE block_date,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    tx_sender,
    COALESCE(
        asset_sender,
        tx_sender
    ) AS asset_sender,
    asset_receiver AS receiver,
    asset.asset_id AS asset_id,
    IFNULL(
        CASE
            WHEN asset.decimals > 0 THEN asset_amount :: FLOAT / pow(
                10,
                asset.decimals
            )
            ELSE asset_amount :: FLOAT
        END,
        0
    ) AS amount,
    asset_name,
    decimals,
    IFNULL(
        fee,
        0
    ) AS fee,
    'axfer' AS tx_type,
    'asset transfer' AS tx_type_name,
    tx_message,
    extra
FROM
    {{ ref('core__fact_transaction') }}
    assetTransfer
    JOIN {{ ref('core__dim_asset') }}
    asset
    ON assetTransfer.dim_asset_id = asset.dim_asset_id
WHERE
    dim_transaction_type_id = 'c495d86d106bb9c67e5925d952e553f2'
