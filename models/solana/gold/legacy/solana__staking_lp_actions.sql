{{ config(
    materialized = 'view', 
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_lp_actions']
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
    event_type, 
    instruction, 
    ingested_at

FROM {{ ref('silver_solana__staking_lp_actions') }} 
