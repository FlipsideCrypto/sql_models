{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_blocks']
) }}

SELECT
    block_id, 
    block_timestamp, 
    network, 
    blockchain, 
    tx_count, 
    block_height, 
    block_time, 
    blockhash, 
    previous_block_id, 
    previous_blockhash, 
    ingested_at
FROM 
    {{ ref('silver_solana__blocks') }}