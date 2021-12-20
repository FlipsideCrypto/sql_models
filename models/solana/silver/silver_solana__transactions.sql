{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_solana', 'solana_transactions']
) }}

WITH base_table as (
  SELECT 
    block_timestamp :: TIMESTAMP AS block_timestamp, 
    block_id :: INTEGER AS block_id,
    tx :transaction:message:recentBlockhash :: STRING AS recent_blockhash, 
    tx_id :: STRING AS tx_id,
    pre.value :mint :: STRING as pre_mint,
    post.value :mint :: STRING as post_mint,
    pre.value :owner :: STRING AS tx_from_address, 
    post.value :owner :: STRING AS tx_to_address, 
    tx :meta:fee :: INTEGER AS fee, -- This is in lamports right now
    tx :meta:status:Err :: ARRAY AS error, -- Need some sort of coalesce statement here 
    ins.value :programId :: STRING AS program_id, 
    ingested_at :: TIMESTAMP AS ingested_at
FROM "FLIPSIDE_DEV_DB"."BRONZE_SOLANA"."SOLANA_TXS",  
    LATERAL FLATTEN (
        input => tx :meta:preTokenBalances
    ) pre, 
    LATERAL FLATTEN (
        input => tx :meta:postTokenBalances
    ) post, 
    LATERAL FLATTEN (
        input => tx :transaction:message:instructions
    ) ins

WHERE 
  1 = 1
AND program_id <> 'Vote111111111111111111111111111111111111111'
)
 

SELECT  
  block_timestamp, 
  block_id, 
  recent_blockhash, 
  tx_id,
  pre_mint,
  post_mint, 
  tx_from_address, 
  tx_to_address, 
  fee, 
  error, 
  program_id, 
  ingested_at
  FROM (
    SELECT 
        *, 
        row_number() OVER (PARTITION BY block_id, tx_id ORDER BY ingested_at DESC) AS rn
    FROM base_table
  ) sq
  WHERE 
    sq.rn = 1 
  
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

  


