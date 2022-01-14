{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_blockhash, 
    tx_id, 
    tx_from_address, 
    tx_to_address, 
    succeeded, 
    event_meta, 
    postTokenBalances, 
    event_info, 
    ingested_at, 
    transfer_tx_flag

FROM {{ ref('silver_solana__events') }} 

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1