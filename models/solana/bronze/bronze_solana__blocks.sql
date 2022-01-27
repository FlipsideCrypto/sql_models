{{ config (
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'bronze_solana', 'solana_blocks']
) }}

SELECT 
    record_id, 
    offset_id AS block_id, 
    block_id AS offset_id, 
    block_timestamp, 
    network, 
    chain_id, 
    tx_count, 
    header, 
    ingested_at
FROM 
    {{ source(
      'prod',
      'solana_blocks'
    ) }} 