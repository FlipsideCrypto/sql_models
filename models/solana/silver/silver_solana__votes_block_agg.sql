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
    count(block_id) AS num_votes, 
    ingested_at
  FROM {{ ref('silver_solana__votes') }}

  {% if is_incremental() %}
     WHERE ingested_at >= getdate() - interval '2 days'
  {% endif %}

  GROUP BY block_id, blockchain, ingested_at
)

SELECT 
  t.block_timestamp,
  v.block_id, 
  v.blockchain, 
  num_votes

FROM v

LEFT OUTER JOIN {{ ref('bronze_solana__transactions') }} t
ON v.block_id = t.block_id

qualify(ROW_NUMBER() over(PARTITION BY v.block_id
ORDER BY
  t.block_timestamp DESC)) = 1