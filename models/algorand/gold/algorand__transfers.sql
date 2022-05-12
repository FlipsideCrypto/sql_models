{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'transactions', 'transfers', 'gold'],
) }}

SELECT
    block_timestamp,
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_sender,
    tx_sender,
    receiver,
    asset_id,
    amount,
    fee,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__transfers') }}
