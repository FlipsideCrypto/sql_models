{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_votes']
) }}

WITH base_table as (
  SELECT 
    block_id :: INTEGER AS block_id,
    chain_id :: STRING AS blockchain, 
    tx_id :: STRING AS tx_id, 
    ingested_at :: TIMESTAMP AS ingested_at
  FROM {{ ref('bronze_solana__transactions') }}

  WHERE tx :transaction:message:instructions[0]:programId :: STRING = 'Vote111111111111111111111111111111111111111'

  {% if is_incremental() %}
     AND ingested_at >= getdate() - interval '2 days'
   {% endif %}

  qualify(ROW_NUMBER() over(PARTITION BY block_id :: INTEGER, tx_id :: STRING
  ORDER BY
  ingested_at :: TIMESTAMP DESC)) = 1
), 

v AS (
  SELECT 
    block_id, 
    blockchain, 
    count(block_id) AS num_votes
  FROM base_table
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