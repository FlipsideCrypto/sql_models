{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events', 'solana_votes']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_block_hash, 
    tx_id, 
    succeeded,  
    program_type, 
    program_id, 
    vote_account, 
    vote_authority, 
    ingested_at, 
    transfer_tx_flag
FROM {{ ref('silver_solana__votes') }} 

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1