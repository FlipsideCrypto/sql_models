{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_airdrops']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_block_hash, 
    tx_id, 
    succeeded, 
    authority, 
    destination, 
    mint, 
    source, 
    preTokenBalances,  
    postTokenBalances, 
    instruction, 
    ingested_at

FROM {{ ref('silver_solana__airdrops') }} 