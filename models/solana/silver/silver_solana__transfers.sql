{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_transfers']
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
  i.value :parsed:info:destination :: STRING AS destination, 
  i.value :parsed:info:source :: STRING AS source, 
  i.value :parsed:info:authority :: STRING AS authority,
  CASE 
    WHEN i.event_type = 'transfer' AND i.value:programId :: STRING = '11111111111111111111111111111111' THEN i.value:parsed:info:lamports/POW(10,9)
    ELSE i.value :parsed:info:amount/POW(10,6) END
  AS amount,
  i.event_type :: STRING AS event_type, 
  i.value AS instruction, 
  t.ingested_at :: TIMESTAMP AS ingested_at

FROM {{ ref('solana_dbt__instructions') }} i

LEFT OUTER JOIN {{ ref('bronze_solana__transactions') }} t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

WHERE i.event_type :: STRING = 'transfer'

{% if is_incremental() %}
  AND t.ingested_at >= getdate() - interval '2 days'
  AND i.ingested_at >= getdate() - interval '2 days'
{% endif %}  

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id
ORDER BY
  t.ingested_at DESC)) = 1