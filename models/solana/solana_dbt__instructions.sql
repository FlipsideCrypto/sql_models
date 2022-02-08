{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, e.index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['ingested_at::DATE'],
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
WHERE COALESCE(
  e.value:parsed:type:: STRING, 
  '' 
  ) <> 'vote'

AND COALESCE(
  e.value:programId :: STRING, 
  '') NOT IN ('FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH', 'DtmE9D2CSB4L5D6A15mraeEjrGMm6auWVzgaD8hK2tZM')

{% if is_incremental() %}
  AND ingested_at >= getdate() - interval '2 days'
{% endif %}