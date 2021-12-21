{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_blocks']
) }}

SELECT
    offset_id, 
    block_id, 
    block_timestamp, 
    network, 
    blockchain, 
    tx_count, 
    block_height, 
    block_time, 
    blockhash, 
    parent_slot, 
    previous_blockhash, 
    ingested_at
FROM 
    {{ ref('silver_solana__blocks') }}