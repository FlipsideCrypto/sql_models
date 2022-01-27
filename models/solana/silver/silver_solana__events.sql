{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_events']
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
  i.index :: INTEGER AS index, 
  i.event_type :: STRING AS event_type, 
  i.value AS instruction, 
  ii.value as inner_instruction,
  t.ingested_at :: TIMESTAMP AS ingested_at
FROM {{ ref('solana_dbt__instructions') }} i

LEFT OUTER JOIN {{ ref('solana_dbt__inner_instructions') }} ii 
ON ii.block_id = i.block_id 
AND ii.tx_id = i.tx_id 
AND ii.mapped_event_index = i.index

LEFT OUTER JOIN {{ ref('bronze_solana__transactions') }} t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

   {% if is_incremental() %}
    WHERE t.ingested_at >= (
      SELECT
        MAX(
          ingested_at
        )
      FROM
        {{ this }}
    )
    {% endif %}

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id, i.index
ORDER BY
  i.index DESC)) = 1