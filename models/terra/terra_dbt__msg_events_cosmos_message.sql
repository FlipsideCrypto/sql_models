{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'terra', 'cosmos']
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
  event_attributes :action :: STRING AS amount,
  event_attributes :module :: STRING AS module,
  event_attributes :sender :: STRING AS sender
FROM
  {{ ref('silver_terra__msg_events') }}
WHERE
  msg_module = 'cosmos'
  AND event_type = 'message'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
