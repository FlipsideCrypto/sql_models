{{ config(
   materialized='incremental',
   incremental_strategy='delete+insert',
   unique_key='block_number || tx_id', 
   tags=['snowflake', 'gold_flow', 'gold', 'gold__flow_staking']) }}


-- delegator_tokens_committed
--
-- - tokens_withdrawn is delegator + amount
-- - delegator_tokens_committed reciever is validator
-- - call this "stake"
WITH flow_labels AS (
    SELECT 
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM 
    {{ source(
        'shared',
        'udm_address_labels'
    )}}
    WHERE blockchain = 'flow'
)
SELECT
'flow' as blockchain,
e.block_timestamp,
e.block_number,
e.tx_id,
d.delegator_address as event_from,
event_from_label_type,
event_from_label_subtype,
event_from_label,
event_from_address_name,
event_to,
event_to_labels.l1_label as event_to_label_type,
event_to_labels.l2_label as event_to_label_subtype,
event_to_labels.project_name as event_to_label,
event_to_labels.address_name as event_to_address_name,
'stake' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM
{{ ref('gold__flow_events') }} e
LEFT OUTER JOIN
  {{ ref('gold__flow_delegator_addresses')}} as d
ON
e.event_from = d.delegator_id
AND e.event_to = d.node_id
LEFT OUTER JOIN
  flow_labels as event_to_labels
ON
  d.delegator_address = event_to_labels.address
WHERE
e.event_type = 'delegator_tokens_committed'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION ALL
--
-- delegator_unstaked_tokens_withdrawn
-- - tokens_deposited event is delegator/reciever
-- - delegator_unstaked_tokens_withdrawn has the validator as sender
SELECT
'flow' as blockchain,
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
'unstake' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM
{{ ref('gold__flow_events') }} e
LEFT OUTER JOIN
  {{ ref('gold__flow_delegator_addresses')}} as d
ON
e.event_to = d.delegator_id
AND e.event_from = d.node_id
LEFT OUTER JOIN
  flow_labels as event_to_labels
ON
  d.delegator_address = event_to_labels.address
WHERE
e.event_type = 'delegator_tokens_unstaked'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION ALL


SELECT
'flow' as blockchain,
e.block_timestamp,
block_number,
e.tx_id,
d.delegator_address as event_from,
event_from_labels.l1_label as event_from_label_type,
event_from_labels.l2_label as event_from_label_subtype,
event_from_labels.project_name as event_from_label,
event_from_labels.address_name as event_from_address_name,
event_to,
event_to_labels.l1_label as event_to_label_type,
event_to_labels.l2_label as event_to_label_subtype,
event_to_labels.project_name as event_to_label,
event_to_labels.address_name as event_to_address_name,
'stake' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM
{{ ref('gold__flow_events') }} e
JOIN {{ ref('gold__flow_delegator_addresses')}} d
ON d.delegator_id = 0
AND e.event_to = d.node_id
LEFT OUTER JOIN
  flow_labels as event_from_labels
ON d.delegator_address = event_from_labels.address
LEFT OUTER JOIN
  flow_labels as event_to_labels
ON
  event_to = event_to_labels.address
WHERE e.event_type = 'tokens_staked'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION ALL


SELECT
'flow' as blockchain,
e.block_timestamp,
block_number,
e.tx_id,
event_from,
event_from_labels.l1_label as event_from_label_type,
event_from_labels.l2_label as event_from_label_subtype,
event_from_labels.project_name as event_from_label,
event_from_labels.address_name as event_from_address_name,
d.delegator_address as event_to,
event_to_labels.l1_label as event_to_label_type,
event_to_labels.l2_label as event_to_label_subtype,
event_to_labels.project_name as event_to_label,
event_to_labels.address_name as event_to_address_name,
'unstake' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM
{{ ref('gold__flow_events') }} e
JOIN {{ ref('gold__flow_delegator_addresses')}} d
ON d.delegator_id = 0
AND e.event_to = d.node_id
LEFT OUTER JOIN
  flow_labels as event_to_labels
ON d.delegator_address = event_to_labels.address
LEFT OUTER JOIN
  flow_labels as event_from_labels
ON
  event_from = event_from_labels.address
WHERE e.event_type = 'tokens_unstaked'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION ALL
-- unstaked_tokens_withdrawn
-- need to combine two events here to get the correct to and from
-- unstaked_tokens_withdrawn has validator info
-- tokens_deposited has the delegator info
SELECT
blockchain,
block_timestamp,
block_number,
v.tx_id,
event_from,
event_from_label_type,
event_from_label_subtype,
event_from_label,
event_from_address_name,
event_to,
event_to_label_type,
event_to_label_subtype,
event_to_label,
event_to_address_name,
'unstake' as event_type,
event_amount,
event_amount_usd,
event_currency
FROM ( -- validator side
  SELECT
  'flow' as blockchain,
  e.block_timestamp,
  e.block_number,
  e.tx_id,
  event_from,
  event_from_label_type,
  event_from_label_subtype,
  event_from_label,
  event_from_address_name
  FROM
  {{ ref('gold__flow_events') }} e
  JOIN
  {{ ref('gold__flow_transactions') }} t
  ON
  e.tx_id = t.tx_id
  WHERE
  t.tx_type = 'unstaked_tokens_withdrawn'
  AND e.event_type = 'unstaked_tokens_withdrawn'
  {% if is_incremental() %}
    AND e.block_timestamp >= getdate() - interval '1 days'
  {% else %}
    AND e.block_timestamp >= getdate() - interval '9 months'
  {% endif %}
) v
JOIN ( -- delegator side
  SELECT
  e.tx_id,
  event_to,
  event_to_label_type,
  event_to_label_subtype,
  event_to_label,
  event_to_address_name,
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
    flow_labels as event_to_labels
  ON
    d.delegator_address = event_to_labels.address
  WHERE
  t.tx_type = 'unstaked_tokens_withdrawn'
  AND e.event_type = 'tokens_deposited'
  {% if is_incremental() %}
    AND e.block_timestamp >= getdate() - interval '1 days'
  {% else %}
    AND e.block_timestamp >= getdate() - interval '9 months'
  {% endif %}
) d
ON
v.tx_id = d.tx_id


