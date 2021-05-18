{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'flow', 'events']
  )
}}


SELECT
  'flow' as blockchain,
  block_timestamp,
  block_id as block_number,
  tx_id,
  tx_from,
  tx_from_labels.l1_label as tx_from_label_type,
  tx_from_labels.l2_label as tx_from_label_subtype,
  tx_from_labels.project_name as tx_from_label,
  tx_from_labels.address_name as tx_from_address_name,
  tx_to,
  tx_to_labels.l1_label as tx_to_label_type,
  tx_to_labels.l2_label as tx_to_label_subtype,
  tx_to_labels.project_name as tx_to_label,
  tx_to_labels.address_name as tx_to_address_name,
  tx_type,
  tx_fee,
  NULL::float as tx_fee_usd,
  event_from,
  event_from_labels.l1_label as event_from_label_type,
  event_from_labels.l2_label as event_from_label_subtype,
  event_from_labels.project_name as event_from_label,
  event_from_labels.address_name as event_from_address_name,
  event_to,
  event_to_labels.l1_label as event_to_label_type,
  event_to_labels.l2_label as event_to_label_subtype,
  event_to_labels.project_name as event_to_label,
  event_to_labels.address_name as event_to_address_name,
  event_type,
  event_amount / pow(10, COALESCE(adj.decimal_adjustment, 0)) as event_amount,
  NULL::float as event_amount_usd,
  coalesce(adj.symbol, event_currency) as event_currency
FROM {{ source('flow', 'udm_events_flow')}} e

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels')}} as tx_from_labels
    ON e.tx_from = tx_from_labels.address AND tx_from_labels.blockchain = 'flow'

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels')}} as tx_to_labels
    ON e.tx_to = tx_to_labels.address AND tx_to_labels.blockchain = 'flow'

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels')}} as event_from_labels
    ON e.event_from = event_from_labels.address AND event_from_labels.blockchain = 'flow'

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels')}} as event_to_labels
    ON e.event_to = event_to_labels.address AND event_to_labels.blockchain = 'flow'

LEFT OUTER JOIN
  {{ source('shared', 'udm_decimal_adjustments')}} adj
    ON e.event_currency = adj.token_identifier AND adj.blockchain = 'flow'

WHERE 
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '7 days'
  {% else %}
    block_timestamp >= getdate() - interval '9 months'
  {% endif %}
