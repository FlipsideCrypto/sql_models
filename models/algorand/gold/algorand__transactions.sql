{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'transactions', 'gold'],
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
    tx_type,
    tx_type_name,
    genisis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__transactions') }}
