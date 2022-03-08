{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_nfts']
) }}

WITH base_i AS (
  SELECT
    block_id, 
    tx_id, 
    index :: INTEGER AS index, 
    value, 
    ingested_at
  FROM {{ ref('solana_dbt__instructions') }} 

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}
), 

base_ii AS (
  SELECT
    block_id, 
    tx_id, 
    mapped_event_index :: INTEGER AS mapped_event_index, 
    value,  
    ingested_at
  FROM {{ ref('solana_dbt__inner_instructions') }}

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
  t.tx :meta:postTokenBalances[0]:mint :: STRING AS mint, 
  CASE WHEN t.tx :meta:status:Err IS NULL THEN TRUE ELSE FALSE END AS succeeded, 
  t.tx :meta:preTokenBalances :: ARRAY AS preTokenBalances, 
  t.tx :meta:postTokenBalances :: ARRAY AS postTokenBalances,   
  i.index :: INTEGER AS index, 
  i.value :programId :: STRING AS program_id, 
  i.value AS instruction, 
  ii.value as inner_instruction,
  t.ingested_at :: TIMESTAMP AS ingested_at
FROM base_i i

LEFT OUTER JOIN base_ii ii 
ON ii.block_id = i.block_id 
AND ii.tx_id = i.tx_id 
AND ii.mapped_event_index = i.index

LEFT OUTER JOIN base_t t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

WHERE i.value:programId :: STRING IN ('MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8', 
                                      '617jbWo616ggkDxvW1Le8pV38XLbVSyWY8ae6QUmGBAU', 
                                      'CJsLwbP1iu5DuUikHEJnLfANgKy6stB2uFgvBBHoyxwz', 
                                      'AmK5g2XcyptVLCFESBCJqoSfwV3znGoVYQnqEnaAZKWn', 
                                      '2CkRtcdfBTxRrCZxJ81NbiMYytsmt2eRUGq7xmAwoRyyjALj231CtW8qSPp2Lv2mhChrWeEcDRf5x3n28f3y3oBx', 
                                      'SPf5WqNywtPrRXSU5enq5z9bPPhREaSYf2LhN5fUxcj', 
                                      '2k8iJk39MtwMVEDMNuvUpEsm2jhBb8678jAqQkGEhu3bxPW4HesVkdJzMuMvgn61ST1S5YpskxVNaPDhrheUmjz9', 
                                      'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ', 
                                      'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp', 
                                      'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')

AND  t.tx :meta:postTokenBalances[0]:mint :: STRING IS NOT NULL

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id, i.index
ORDER BY
  t.ingested_at DESC)) = 1