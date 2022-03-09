{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  persist_docs={"relation": true, "columns": true}, 
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_LP']
) }}

WITH base_i AS (
  SELECT
    block_id, 
    tx_id, 
    index :: INTEGER AS index, 
    event_type, 
    value, 
    ingested_at
  FROM {{ ref('solana_dbt__instructions') }} 

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}
), 

base_t AS (
  SELECT
    block_timestamp, 
    block_id, 
    tx_id, 
    chain_id, 
    tx, 
    ingested_at

  FROM {{ ref('bronze_solana__transactions') }}

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}
)

SELECT 
  t.block_timestamp :: TIMESTAMP AS block_timestamp, 
  t.block_id :: INTEGER AS block_id,
  t.chain_id :: STRING AS blockchain, 
  t.tx :transaction:message:recentBlockhash :: STRING AS recent_block_hash, 
  t.tx_id :: STRING AS tx_id,
  CASE WHEN t.tx :meta:status:Err IS NULL THEN TRUE ELSE FALSE END AS succeeded, 
  t.tx :meta:preTokenBalances :: ARRAY AS preTokenBalances, 
  t.tx :meta:postTokenBalances :: ARRAY AS postTokenBalances,   
  i.event_type :: STRING AS event_type, 
  i.value AS instruction, 
  t.ingested_at :: TIMESTAMP AS ingested_at
FROM base_i i

LEFT OUTER JOIN base_t t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

WHERE i.event_type :: STRING IN ('initialize', 'split', 'deactivate', 'delegate', 'withdraw', 'merge', 'authorize', 'allocate', 'assign', 'setLockup')

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id
ORDER BY
  t.ingested_at DESC)) = 1