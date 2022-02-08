{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_nfts']
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

{% if is_incremental() %}
  AND ii.ingested_at >= getdate() - interval '2 days'
{% endif %}

LEFT OUTER JOIN {{ ref('bronze_solana__transactions') }} t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

  {% if is_incremental() %}
    WHERE t.ingested_at >= getdate() - interval '2 days'
    AND i.ingested_at >= getdate() - interval '2 days'
  {% endif %}

AND i.value:programId :: STRING IN ('MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8', 
                                      '617jbWo616ggkDxvW1Le8pV38XLbVSyWY8ae6QUmGBAU', 
                                      'CJsLwbP1iu5DuUikHEJnLfANgKy6stB2uFgvBBHoyxwz', 
                                      'AmK5g2XcyptVLCFESBCJqoSfwV3znGoVYQnqEnaAZKWn', 
                                      '2CkRtcdfBTxRrCZxJ81NbiMYytsmt2eRUGq7xmAwoRyyjALj231CtW8qSPp2Lv2mhChrWeEcDRf5x3n28f3y3oBx', 
                                      'SPf5WqNywtPrRXSU5enq5z9bPPhREaSYf2LhN5fUxcj', 
                                      '2k8iJk39MtwMVEDMNuvUpEsm2jhBb8678jAqQkGEhu3bxPW4HesVkdJzMuMvgn61ST1S5YpskxVNaPDhrheUmjz9', 
                                      'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ')

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id, i.index
ORDER BY
  t.ingested_at DESC)) = 1