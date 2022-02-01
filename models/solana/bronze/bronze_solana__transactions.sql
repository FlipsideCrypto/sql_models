{{ config (
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'bronze_solana', 'solana_transactions']
) }}

SELECT
   record_id, 
   tx_id, 
   tx_block_index, 
   offset_id AS block_id, 
   block_id AS offset_id, 
   block_timestamp, 
   network, 
   chain_id, 
   tx, 
   ingested_at
FROM 
    {{ source(
      'prod',
      'solana_txs'
    ) }} 