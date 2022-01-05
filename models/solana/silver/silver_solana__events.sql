{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_events']
) }}

WITH base_table as (
  SELECT 
    block_timestamp :: TIMESTAMP AS block_timestamp, 
    offset_id :: INTEGER AS block_id,
    chain_id :: STRING AS blockchain, 
    tx :transaction:message:recentBlockhash :: STRING AS recent_blockhash, 
    tx_id :: STRING AS tx_id,
    tx :meta:preTokenBalances[0]:owner :: STRING AS tx_from_address, 
    tx :meta:postTokenBalances[0]:owner :: STRING AS tx_to_address, 
    CASE WHEN tx :meta:status:Err IS NULL THEN TRUE ELSE FALSE END AS succeeded,  
    tx :transaction:message:instructions[0]:parsed:type :: STRING AS event_type,
    tx :transaction:message:instructions[0]:parsed:info :: OBJECT AS event_info, 
    tx :transaction:message:instructions[0]:program :: STRING AS program_type, 
    tx :transaction:message:instructions[0]:programId :: STRING AS program_id, 
    ingested_at :: TIMESTAMP AS ingested_at, 
    CASE WHEN len(tx :meta:postTokenBalances[0]) > 0 AND len(tx :meta:preTokenBalances[0]) > 0 THEN TRUE ELSE FALSE END AS transfer_tx_flag
  FROM {{ ref('bronze_solana__transactions') }}
  WHERE 
    1 = 1

  {% if is_incremental() %}
  AND ingested_at >= (
    SELECT
      MAX(
        ingested_at
      )
    FROM
      {{ this }}
  )
  {% endif %}
)

SELECT  
  block_timestamp, 
  block_id, 
  blockchain, 
  recent_blockhash, 
  tx_id, 
  tx_from_address, 
  tx_to_address, 
  succeeded,
  event_type, 
  event_info, 
  program_type, 
  program_id, 
  ingested_at, 
  transfer_tx_flag

FROM base_table

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1