{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events', 'solana_votes']
) }}

SELECT 
    block_id, 
    block_timestamp, 
    blockchain,
    count(block_id) AS num_votes
FROM {{ ref('silver_solana__votes') }} 

GROUP BY block_id, block_timestamp, blockchain
ORDER BY block_id DESC