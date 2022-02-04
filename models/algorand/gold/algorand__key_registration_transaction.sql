{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'key_registration_transaction', 'gold'],
) }}

SELECT
    block_timestamp,
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_id,
    sender,
    fee,
    participation_key,
    vrf_public_key,
    vote_first,
    vote_last,
    vote_keydilution,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__key_registration_transaction') }}
