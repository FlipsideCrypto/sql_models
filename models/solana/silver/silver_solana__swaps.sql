{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_swaps']
) }}

SELECT 
  t.block_timestamp :: TIMESTAMP AS block_timestamp, 
  t.block_id :: INTEGER AS block_id,
  t.chain_id :: STRING AS blockchain, 
  t.tx :transaction:message:recentBlockhash :: STRING AS recent_block_hash, 
  t.tx_id :: STRING AS tx_id,
  CASE WHEN t.tx :meta:status:Err IS NULL THEN TRUE ELSE FALSE END AS succeeded, 
  t.tx :meta:preTokenBalances :: ARRAY AS preTokenBalances, 
  t.tx :meta:postTokenBalances :: ARRAY AS postTokenBalances, 
  i.value AS instruction, 
  ii.value AS inner_instruction, 
  t.ingested_at :: TIMESTAMP AS ingested_at
FROM {{ ref('solana_dbt__instructions') }} i

LEFT OUTER JOIN {{ ref('solana_dbt__inner_instructions') }} ii 
ON ii.block_id = i.block_id 
AND ii.tx_id = i.tx_id 
AND ii.mapped_event_index = i.index

{% if is_incremental() %}
  AND ii.ingested_at >= getdate() - interval '2 days'
{% endif %}

LEFT OUTER JOIN {{ ref('bronze_solana__transactions') }} t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

WHERE i.event_type :: STRING = 'transfer'
AND array_size(t.tx :meta:postTokenBalances :: ARRAY) >= 2
AND t.tx :meta:postTokenBalances[0]:mint :: STRING <> t.tx :meta:postTokenBalances[array_size(t.tx :meta:postTokenBalances :: ARRAY)-1]:mint :: STRING
AND t.block_timestamp >= '2022-02-08'

{% if is_incremental() %}
  WHERE t.ingested_at >= getdate() - interval '2 days'
  AND i.ingested_at >= getdate() - interval '2 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id
ORDER BY
  t.ingested_at DESC)) = 1