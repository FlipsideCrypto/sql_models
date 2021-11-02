{{ config(
  materialized='incremental', 
  unique_key='block_timestamp', 
  incremental_strategy='delete+insert',
  cluster_by=['block_timestamp'],
  tags=['snowflake', 'gold', 'near', 'gold__near_events']
)}}
WITH near_labels AS (
  SELECT 
    l1_label,
    l2_label,
    project_name,
    address_name,
    address
  FROM {{ source('shared', 'udm_address_labels') }}
  WHERE blockchain = 'near'
), near_prices AS (
  SELECT
    symbol,
    date_trunc('hour', recorded_at) as hour,
    avg(price) as price
  FROM {{ source('shared', 'prices') }}
  WHERE symbol = 'NEAR'
  GROUP BY symbol, hour
), near_decimals AS (
  SELECT *
  FROM {{ source('shared', 'udm_decimal_adjustments') }}
  WHERE blockchain = 'near'
)
SELECT
  e.blockchain,
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
  tx_fee * fee_price.price as tx_fee_usd,
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
  event_amount / POWER(10, COALESCE(adj.decimal_adjustment, 0)) as event_amount,
  (event_amount / POWER(10, COALESCE(adj.decimal_adjustment, 0))) * event_price.price as event_amount_usd,
  event_currency
FROM
  {{ source('near', 'udm_events_near')}} e
LEFT OUTER JOIN near_labels as tx_from_labels ON e.tx_from = tx_from_labels.address
LEFT OUTER JOIN near_labels as tx_to_labels ON e.tx_to = tx_to_labels.address
LEFT OUTER JOIN near_labels as event_from_labels ON e.event_from = event_from_labels.address
LEFT OUTER JOIN near_labels as event_to_labels ON e.event_to = event_to_labels.address
LEFT OUTER JOIN near_prices as fee_price ON fee_price.hour = date_trunc('hour', block_timestamp)
  AND fee_price.symbol = coalesce(e.event_currency, 'NEAR')
LEFT OUTER JOIN near_prices event_price ON event_price.hour = date_trunc('hour', block_timestamp)
  AND event_price.symbol = e.event_currency
LEFT OUTER JOIN near_decimals adj ON e.event_currency = adj.token_identifier
WHERE
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '3 days'
  {% else %}
    block_timestamp >= getdate() - interval '9 months'
  {% endif %}
