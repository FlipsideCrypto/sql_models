{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_transfers', 'solana_user_transfers']
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
  destination, 
  source, 
  authority, 
  amount, 
  event_type, 
  instruction, 
  ingested_at

FROM   
    {{ ref('silver_solana__transfers') }} 