{{ config(materialized='incremental',  unique_key='tx_id || event_from || event_to',
    cluster_by=['block_timestamp'],
tags=['events', 'flow', 'rewards','snowflake','gold']) }}

WITH  event_to_labels as (
	SELECT * FROM {{source('shared','udm_address_labels')}}
 WHERE blockchain = 'flow')
SELECT
'flow' as blockchain,
e.block_timestamp,
e.block_number,
e.tx_id,
e.event_from,
event_from_label_type,
event_from_label_subtype,
event_from_label,
event_from_address_name,
d.delegator_address as event_to,
event_to_labels.l1_label as event_to_label_type,
event_to_labels.l2_label as event_to_label_subtype,
event_to_labels.project_name as event_to_label,
event_to_labels.address_name as event_to_address_name,
'reward_withdrawn' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM
{{ ref('gold__flow_events') }} e
JOIN
{{ ref('gold__flow_transactions') }} t
ON
e.tx_id = t.tx_id
LEFT OUTER JOIN
  {{ ref('gold__flow_delegator_addresses')}} as d
ON
e.event_to = d.delegator_id
AND e.event_from = d.node_id
LEFT OUTER JOIN
  event_to_labels
ON
  d.delegator_address = event_to_labels.address
WHERE
t.tx_type = 'reward_tokens_withdrawn'
AND e.event_type = 'delegator_reward_tokens_withdrawn'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
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
{{ ref('gold__flow_events') }} e
JOIN
{{ ref('gold__flow_transactions') }} t
ON
e.tx_id = t.tx_id
WHERE
t.tx_type = 'rewards_paid'
AND e.event_type = 'rewards_paid'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION ALL


-- delegator_rewards_paid
SELECT
e.blockchain,
e.block_timestamp,
e.block_number,
e.tx_id,
event_from,
event_from_label_type,
event_from_label_subtype,
event_from_label,
event_from_address_name,
d.delegator_address as event_to,
event_to_labels.l1_label as event_to_label_type,
event_to_labels.l2_label as event_to_label_subtype,
event_to_labels.project_name as event_to_label,
event_to_labels.address_name as event_to_address_name,
'rewards_paid' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM
{{ ref('gold__flow_events') }} e
JOIN
{{ ref('gold__flow_transactions') }} t
ON
e.tx_id = t.tx_id
LEFT OUTER JOIN
  {{ ref('gold__flow_delegator_addresses')}} as d
ON
e.event_to = d.delegator_id
AND e.event_from = d.node_id
LEFT OUTER JOIN
 event_to_labels
ON
  d.delegator_address = event_to_labels.address
WHERE
t.tx_type = 'rewards_paid'
AND e.event_type = 'delegator_rewards_paid'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}