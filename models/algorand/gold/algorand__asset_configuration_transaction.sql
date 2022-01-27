{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'asset_configuration_transaction', 'gold'],
) }}

SELECT
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_id,
    asset_supply,
    sender,
    fee,
    asset_parameters,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__asset_configuration_transaction') }}
