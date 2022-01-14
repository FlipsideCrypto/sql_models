{{ config(
    materialized = 'view',
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events', 'solana_swaps']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_blockhash, 
    tx_id,
    sender,
    token_sent,     
    amount_sent, 
    token_received,  
    amount_received, 
    succeeded, 
    ingested_at
    
FROM {{ ref('silver_solana__swaps') }} 

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1