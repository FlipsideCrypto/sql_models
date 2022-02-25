{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_airdrops']
) }}


WITH base_i AS (
  SELECT
    block_id, 
    tx_id, 
    index :: INTEGER AS index, 
    event_type, 
    value, 
    ingested_at
  FROM {{ ref('solana_dbt__instructions') }} 

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
    program_id, 
    account_keys, 
    ingested_at

  FROM {{ ref('solana_dbt__transactions') }}

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}
)

SELECT 
  t.block_timestamp, 
  t.block_id,
  t.chain_id AS blockchain, 
  t.recent_block_hash, 
  t.tx_id,
  t.program_id, 
  t.succeeded, 
  i.value :parsed:info:authority :: STRING AS authority, 
  i.value :parsed:info:destination :: STRING AS destination, 
  i.value :parsed:info:mint :: STRING AS mint, 
  i.value :parsed:info:source :: STRING AS source, 
  t.tx :meta:preTokenBalances :: ARRAY AS preTokenBalances, 
  t.tx :meta:postTokenBalances :: ARRAY AS postTokenBalances,   
  i.value AS instruction, 
  t.ingested_at :: TIMESTAMP AS ingested_at, 
  t.account_keys AS account_keys
FROM base_i i

LEFT OUTER JOIN base_t t 
ON t.block_id = i.block_id 
AND t.tx_id = i.tx_id

WHERE i.event_type :: STRING = 'transferChecked'

qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id
ORDER BY
  t.ingested_at DESC)) = 1