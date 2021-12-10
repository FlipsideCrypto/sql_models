{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'asset_transfer_transaction', 'gold'],
) }}

SELECT
    intra,
    block_id,
    tx_group_id,
    tx_id,
    asset_id,
    sender,
    fee,
    asset_sender,
    asset_reciever,
    asset_amount,
    tx_type,
    tx_type_name,
    genisis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__asset_transfer_transaction') }}
