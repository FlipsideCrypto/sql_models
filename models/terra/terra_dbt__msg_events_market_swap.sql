{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'terra', 'swap']
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
  event_attributes :offer :: STRING AS offer,
  event_attributes :swap_coin :: STRING AS swap_coin,
  event_attributes :swap_fee :: STRING AS swap_fee,
  event_attributes :trader :: STRING AS trader
FROM
  {{ ref('silver_terra__msg_events') }}
WHERE
  msg_module = 'market'
  AND event_type = 'swap'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
