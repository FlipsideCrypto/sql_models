{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'oracles']
) }}

SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  tx_type,
  msg_module,
  msg_type,
  event_type,
  event_attributes,
  event_attributes :denom :: STRING AS currency,
  event_attributes :feeder :: STRING AS feeder,
  event_attributes :voter :: STRING AS voter
FROM
  {{ ref('silver_terra__msg_events') }}
WHERE
  msg_module = 'oracle'
  AND event_type = 'vote'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
