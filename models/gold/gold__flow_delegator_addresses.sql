{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "CONCAT_WS('-',block_id, tx_id, node_id)",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'gold_flow', 'gold', 'gold__flow_delegator_addresses']
) }}

SELECT
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.tx_to AS delegator_address,
  n.event_from AS delegator_id,
  n.event_to AS node_id
FROM
  {{ source(
    'flow',
    'udm_events_flow'
  ) }}
  t
  JOIN {{ source(
    'flow',
    'udm_events_flow'
  ) }}
  n
  ON t.tx_id = n.tx_id
WHERE
  n.event_type = 'new_delegator_created'
  AND t.tx_to IS NOT NULL

{% if is_incremental() %}
AND t.block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  AND t.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
UNION
SELECT
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.event_from AS delegator_address,
  '0' AS delegator_id,
  t.event_to AS node_id
FROM
  {{ source(
    'flow',
    'udm_events_flow'
  ) }}
  t
WHERE
  t.event_type = 'new_node_created'

{% if is_incremental() %}
AND t.block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  AND t.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
