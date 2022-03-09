{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  persist_docs={"relation": true, "columns": true}, 
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_votes']
) }}

WITH v AS (
  SELECT 
    block_id, 
    blockchain, 
    count(block_id) AS num_votes
  FROM {{ ref('silver_solana__votes') }}
  
  {% if is_incremental() %}
     WHERE ingested_at >= getdate() - interval '2 days'
  {% endif %}

  GROUP BY block_id, blockchain
)

SELECT 
  t.block_timestamp,
  v.block_id, 
  v.blockchain, 
  num_votes

FROM v

LEFT OUTER JOIN (SELECT DISTINCT block_id, block_timestamp FROM {{ ref('bronze_solana__transactions') }}) t 
ON v.block_id = t.block_id

