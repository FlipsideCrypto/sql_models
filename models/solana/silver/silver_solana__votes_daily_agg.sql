{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_votes']
) }}

WITH v AS (
  SELECT 
    block_id, 
    blockchain, 
    count(block_id) AS num_votes
  FROM {{ ref('silver_solana__votes') }}
  GROUP BY block_id, blockchain 
)

SELECT 
  b.block_timestamp,
  v.block_id, 
  v.blockchain, 
  num_votes
FROM v

LEFT OUTER JOIN {{ ref('silver_solana__blocks') }} b
ON b.block_id = v.block_id