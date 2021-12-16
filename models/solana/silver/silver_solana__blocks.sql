{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_solana', 'solana_blocks']
) }}

WITH base_tables AS (
  SELECT  
      *
  FROM 
  (
      SELECT 
          *, 
          row_number() OVER (PARTITION BY block_id ORDER BY ingested_at DESC) AS rn
      FROM {{ source('bronze_solana', 'solana_blocks') }}
  ) sq
  WHERE sq.rn = 1 
)

SELECT
    record_id :: VARCHAR AS record_id, 
    offset_id :: INTEGER AS offset_id, 
    block_id :: INTEGER AS block_id, 
    block_timestamp :: TIMESTAMP AS block_timestamp, 
    network :: STRING AS network, 
    chain_id :: STRING AS blockchain, 
    tx_count :: INTEGER AS tx_count,
    header :blockHeight :: INTEGER AS block_height, 
    header :blockTime :: INTEGER AS block_time, 
    header :blockhash :: VARCHAR AS blockhash, 
    header :parentSlot :: INTEGER AS parent_slot, 
    header :previousBlockhash :: VARCHAR AS previous_blockhash,  
    ingested_at :: TIMESTAMP AS ingested_at
FROM 
   base_tables 
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