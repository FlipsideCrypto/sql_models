{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, event_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_events']
) }}

SELECT 
    block_timestamp,
    block_id, 
    tx_id, 
    e.index, 
    e.value:parsed:type:: STRING AS event_type, 
    e.value, 
    ingested_at
FROM {{ ref('bronze_solana__transactions') }} t,
table(flatten(tx:transaction:message:instructions)) AS e
WHERE e.value:parsed:type:: STRING <> 'vote'

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