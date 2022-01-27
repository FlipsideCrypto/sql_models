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

  {% if is_incremental() %}
    WHERE ingested_at >= (
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
    block_id :: INTEGER AS block_id,  
    block_timestamp :: TIMESTAMP AS block_timestamp, 
    network :: STRING AS network, 
    chain_id :: STRING AS blockchain, 
    tx_count :: INTEGER AS tx_count,
    header :blockHeight :: INTEGER AS block_height, 
    header :blockTime :: INTEGER AS block_time, 
    header :blockhash :: VARCHAR AS block_hash, 
    header :parentSlot :: INTEGER AS previous_block_id, 
    header :previousBlockhash :: VARCHAR AS previous_block_hash,  
    ingested_at :: TIMESTAMP AS ingested_at
FROM 
   base_tables 

 qualify(ROW_NUMBER() over(PARTITION BY block_id
  ORDER BY
    ingested_at DESC)) = 1