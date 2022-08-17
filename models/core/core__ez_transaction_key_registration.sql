{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    block_timestamp :: DATE block_date,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    b.tx_sender,
    fee,
    participation_key,
    vrf_public_key,
    vote_first,
    vote_last,
    vote_keydilution,
    'keyreg' AS tx_type,
    'key registration' AS tx_type_name,
    tx_message,
    extra
FROM
    {{ ref('core__fact_transaction') }}
    b
    JOIN {{ ref('core__dim_asset') }}
    ast
    ON b.dim_asset_id = ast.dim_asset_id
WHERE
    b.dim_transaction_type_id = 'c82245dfb0636319da14354856856006'
