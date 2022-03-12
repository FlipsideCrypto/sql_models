{{ config(
    materialized = 'view', 
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_block_hash, 
    tx_id, 
    succeeded, 
    preTokenBalances,  
    postTokenBalances, 
    index,
    event_type, 
    instruction, 
    inner_instruction, 
    ingested_at

FROM {{ ref('silver_solana__events') }} 
