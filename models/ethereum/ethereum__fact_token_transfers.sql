{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'new_eth'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    _log_id
FROM
    {{ source(
        'ethereum_db',
        'fact_token_transfers'
    ) }}
