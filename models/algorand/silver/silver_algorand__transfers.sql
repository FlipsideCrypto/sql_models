{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['snowflake', 'algorand', 'payment','transfer', 'silver_algorand_tx']
) }}

SELECT
    block_timestamp,
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    sender AS tx_sender,
    sender AS asset_sender,
    receiver,
    0 AS asset_id,
    IFNULL(
        amount,
        0
    ) AS amount,
    IFNULL(
        fee,
        0
    ) AS fee,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra,
    concat_ws(
        '-',
        block_id :: STRING,
        intra :: STRING
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver_algorand__payment_transaction') }}
WHERE
    1 = 1

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
UNION
SELECT
    block_timestamp,
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    sender AS tx_sender,
    IFNULL(
        asset_sender,
        sender
    ) AS asset_sender,
    asset_receiver AS receiver,
    assetTransfer.asset_id AS asset_id,
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
    IFNULL(
        fee,
        0
    ) AS fee,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra,
    concat_ws(
        '-',
        assetTransfer.block_id :: STRING,
        assetTransfer.intra :: STRING
    ) AS _unique_key,
    assetTransfer._INSERTED_TIMESTAMP
FROM
    {{ ref('silver_algorand__asset_transfer_transaction') }}
    assetTransfer
    LEFT JOIN {{ ref('silver_algorand__asset') }}
    asset
    ON assetTransfer.asset_transferred = asset.asset_id
WHERE
    1 = 1

{% if is_incremental() %}
AND assetTransfer._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
