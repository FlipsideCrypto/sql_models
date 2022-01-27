{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_blocks']
) }}

WITH base_tables AS (
  SELECT  
      *
  FROM 
    {{ ref('bronze_solana__blocks') }}

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
    offset_id :: INTEGER AS block_id,  
    block_timestamp :: TIMESTAMP AS block_timestamp, 
    network :: STRING AS network, 
    chain_id :: STRING AS blockchain, 
    tx_count :: INTEGER AS tx_count,
    header :blockHeight :: INTEGER AS block_height, 
    header :blockTime :: INTEGER AS block_time, 
    header :blockhash :: VARCHAR AS blockhash, 
    header :parentSlot :: INTEGER AS previous_block_id, 
    header :previousBlockhash :: VARCHAR AS previous_blockhash,  
    ingested_at :: TIMESTAMP AS ingested_at
FROM 
   base_tables 
WHERE 
  1 = 1

 qualify(ROW_NUMBER() over(PARTITION BY block_id
  ORDER BY
    ingested_at DESC)) = 1