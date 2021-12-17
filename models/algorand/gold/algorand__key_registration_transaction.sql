{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'key_registration_transaction', 'gold'],
) }}

SELECT
    intra,
    block_id,
    tx_group_id,
    tx_id,
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
    genisis_hash,
    tx_message,
    extra
FROM
    {{ ref('silver_algorand__key_registration_transaction') }}
