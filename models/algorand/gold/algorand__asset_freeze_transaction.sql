{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'asset_freeze_transaction', 'gold'],
) }}

SELECT
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_id,
    asset_address,
    asset_freeze,
    sender,
    fee,
    tx_type,
    tx_type_name,
    genisis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__asset_freeze_transaction') }}
