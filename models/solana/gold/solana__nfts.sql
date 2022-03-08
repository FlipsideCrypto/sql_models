{{ config(
    materialized = 'view', 
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_nfts']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_block_hash, 
    tx_id,
    mint,  
    succeeded, 
    preTokenBalances,  
    postTokenBalances,
    index,  
    instruction, 
    inner_instruction, 
    ingested_at

FROM {{ ref('silver_solana__nfts') }} 