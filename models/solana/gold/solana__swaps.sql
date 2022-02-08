{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_swaps']
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
    instruction, 
    inner_instruction, 
    ingested_at

FROM {{ ref('silver_solana__swaps') }} 
