{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'asset_transfer_transaction', 'gold'],
) }}

SELECT
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_id,
    sender,
    fee,
    asset_sender,
    asset_receiver,
    asset_amount,
    asset_transferred,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__asset_transfer_transaction') }}
