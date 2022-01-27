{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_transactions']
) }}

WITH base_table as (
  SELECT 
    block_timestamp :: TIMESTAMP AS block_timestamp, 
    block_id :: INTEGER AS block_id,
    tx :transaction:message:recentBlockhash :: STRING AS recent_block_hash, 
    tx_id :: STRING AS tx_id,
    tx :meta:preTokenBalances[0]:mint :: STRING as pre_mint,
    tx :meta:postTokenBalances[0]:mint :: STRING as post_mint,
    tx :meta:preTokenBalances[0]:owner :: STRING AS tx_from_address, 
    tx :meta:postTokenBalances[0]:owner :: STRING AS tx_to_address, 
    tx :meta:fee :: INTEGER AS fee, -- This is in lamports right now
    CASE WHEN tx :meta:status:Err IS NULL THEN TRUE ELSE FALSE END AS succeeded, 
    --tx :meta:status:Err :: ARRAY AS error, -- Need some sort of coalesce statement here 
    tx :transaction:message:instructions[0]:programId :: STRING AS program_id, 
    ingested_at :: TIMESTAMP AS ingested_at, 
    CASE WHEN len(tx :meta:postTokenBalances[0]) > 0 AND len(tx :meta:preTokenBalances[0]) > 0 THEN TRUE ELSE FALSE END AS transfer_tx_flag
FROM {{ ref('bronze_solana__transactions') }}
WHERE 
  1 = 1
AND program_id <> 'Vote111111111111111111111111111111111111111'
AND tx :meta:preTokenBalances[0]:owner :: STRING IS NOT NULL OR tx :meta:postTokenBalances[0]:owner :: STRING IS NOT NULL

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
  recent_block_hash, 
  tx_id,
  pre_mint,
  post_mint, 
  tx_from_address, 
  tx_to_address, 
  fee, 
  succeeded, 
  program_id, 
  ingested_at, 
  transfer_tx_flag

FROM base_table

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1
  

  


