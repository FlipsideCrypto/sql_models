{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "CONCAT_WS('-',tx_id, event_from, event_to)",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['events', 'flow', 'rewards','snowflake','gold', 'address_labels']
) }}

WITH event_to_labels AS (

  SELECT
    *
  FROM
    {{ source(
      'shared',
      'udm_address_labels'
    ) }}
  WHERE
    blockchain = 'flow'
)
SELECT
  'flow' AS blockchain,
  e.block_timestamp,
  e.block_number,
  e.tx_id,
  e.event_from,
  event_from_label_type,
  event_from_label_subtype,
  event_from_label,
  event_from_address_name,
  d.delegator_address AS event_to,
  event_to_labels.l1_label AS event_to_label_type,
  event_to_labels.l2_label AS event_to_label_subtype,
  event_to_labels.project_name AS event_to_label,
  event_to_labels.address_name AS event_to_address_name,
  'reward_withdrawn' AS event_type,
  event_amount,
  event_amount_usd,
  event_currency
FROM
  {{ ref('gold__flow_events') }}
  e
  JOIN {{ ref('gold__flow_transactions') }}
  t
  ON e.tx_id = t.tx_id
  LEFT OUTER JOIN {{ ref('gold__flow_delegator_addresses') }} AS d
  ON e.event_to = d.delegator_id
  AND e.event_from = d.node_id
  LEFT OUTER JOIN event_to_labels
  ON d.delegator_address = event_to_labels.address
WHERE
  t.tx_type = 'reward_tokens_withdrawn'
  AND e.event_type = 'delegator_reward_tokens_withdrawn'

{% if is_incremental() %}
AND e.block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
UNION ALL
  -- rewards_paid to validators
SELECT
  e.blockchain,
  e.block_timestamp,
  e.block_number,
  e.tx_id,
  e.event_from,
  event_from_label_type,
  event_from_label_subtype,
  event_from_label,
  event_from_address_name,
  event_to,
  event_to_label_type,
  event_to_label_subtype,
  event_to_label,
  event_to_address_name,
  event_type,
  event_amount,
  event_amount_usd,
  event_currency
FROM
  {{ ref('gold__flow_events') }}
  e
  JOIN {{ ref('gold__flow_transactions') }}
  t
  ON e.tx_id = t.tx_id
WHERE
  t.tx_type = 'rewards_paid'
  AND e.event_type = 'rewards_paid'

{% if is_incremental() %}
AND e.block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
UNION ALL
  -- delegator_rewards_paid
SELECT
  e.blockchain,
  e.block_timestamp,
  e.block_number,
  e.tx_id,
  e.event_from,
  event_from_label_type,
  event_from_label_subtype,
  event_from_label,
  event_from_address_name,
  d.delegator_address AS event_to,
  event_to_labels.l1_label AS event_to_label_type,
  event_to_labels.l2_label AS event_to_label_subtype,
  event_to_labels.project_name AS event_to_label,
  event_to_labels.address_name AS event_to_address_name,
  'rewards_paid' AS event_type,
  event_amount,
  event_amount_usd,
  event_currency
FROM
  {{ ref('gold__flow_events') }}
  e
  JOIN {{ ref('gold__flow_transactions') }}
  t
  ON e.tx_id = t.tx_id
  LEFT OUTER JOIN {{ ref('gold__flow_delegator_addresses') }} AS d
  ON e.event_to = d.delegator_id
  AND e.event_from = d.node_id
  LEFT OUTER JOIN event_to_labels
  ON d.delegator_address = event_to_labels.address
WHERE
  t.tx_type = 'rewards_paid'
  AND e.event_type = 'delegator_rewards_paid'

{% if is_incremental() %}
AND e.block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
