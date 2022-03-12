{{ config(
    materialized = 'view', 
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events', 'solana_votes']
) }}

SELECT 
    block_id, 
    block_timestamp, 
    blockchain,
    num_votes
FROM {{ ref('silver_solana__votes_block_agg') }} 