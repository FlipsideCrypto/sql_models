{{ config(
    materialized = 'view', 
    persist_docs={"relation": true, "columns": true}, 
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
    block_hash, 
    previous_block_id, 
    previous_block_hash, 
    ingested_at
FROM 
    {{ ref('silver_solana__blocks') }}