{{ config(
    materialized='incremental', 
    unique_key='block_number',
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'gold_flow', 'gold', 'gold__flow_events'])}}


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
),
flow_decimals AS (
    SELECT
        blockchain, 
        token_identifier,
        decimal_adjustment,
        symbol
    FROM
     {{ source('shared', 'udm_decimal_adjustments')}}
    WHERE blockchain = 'flow'
)   
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
FROM
  {{ source('flow', 'udm_events_flow')}} e
LEFT OUTER JOIN
  flow_labels as tx_from_labels
ON
  e.tx_from = tx_from_labels.address
LEFT OUTER JOIN
  flow_labels as tx_to_labels
ON
  e.tx_to = tx_to_labels.address
LEFT OUTER JOIN
  flow_labels as event_from_labels
ON
  e.event_from = event_from_labels.address
LEFT OUTER JOIN
  flow_labels as event_to_labels
ON
  e.event_to = event_to_labels.address
LEFT OUTER JOIN
  flow_decimals adj
ON
  e.event_currency = adj.token_identifier
{% if is_incremental() %}
  WHERE
    block_timestamp >= getdate() - interval '3 days'
  {% endif %}
