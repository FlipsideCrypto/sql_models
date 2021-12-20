{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'gold_solana', 'solana_transactions']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    recent_blockhash, 
    tx_id, 
    pre_mint, 
    post_mint, 
    tx_from_address, 
    tx_to_address, 
    fee, 
    error, 
    program_id, 
    ingested_at
FROM 
    {{ ref('silver_solana__transactions') }}