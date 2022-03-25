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
    event_index,
    contract_address,
    event_name,
    event_inputs,
    topics,
    DATA,
    event_removed,
    _log_id
FROM
    {{ source(
        'ethereum_db',
        'fact_event_logs'
    ) }}
