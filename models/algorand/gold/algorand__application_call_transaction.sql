{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'application_call_transaction', 'gold'],
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
    app_id,
    tx_type,
    tx_type_name,
    genisis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__application_call_transaction') }}
