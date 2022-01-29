{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'gold', 'near', 'gold__near_events', 'address_labels']
) }}

WITH near_labels AS (

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
    ) }}
  WHERE
    blockchain = 'near'
),
near_prices AS (
  SELECT
    symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices_v2'
    ) }}
  WHERE
    symbol = 'NEAR'
  GROUP BY
    symbol,
    HOUR
),
near_decimals AS (
  SELECT
    *
  FROM
    {{ source(
      'shared',
      'udm_decimal_adjustments'
    ) }}
  WHERE
    blockchain = 'near'
)
SELECT
  e.blockchain,
  block_timestamp,
  block_id AS block_number,
  tx_id,
  tx_from,
  tx_from_labels.l1_label AS tx_from_label_type,
  tx_from_labels.l2_label AS tx_from_label_subtype,
  tx_from_labels.project_name AS tx_from_label,
  tx_from_labels.address_name AS tx_from_address_name,
  tx_to,
  tx_to_labels.l1_label AS tx_to_label_type,
  tx_to_labels.l2_label AS tx_to_label_subtype,
  tx_to_labels.project_name AS tx_to_label,
  tx_to_labels.address_name AS tx_to_address_name,
  tx_type,
  tx_fee,
  tx_fee * fee_price.price AS tx_fee_usd,
  event_from,
  event_from_labels.l1_label AS event_from_label_type,
  event_from_labels.l2_label AS event_from_label_subtype,
  event_from_labels.project_name AS event_from_label,
  event_from_labels.address_name AS event_from_address_name,
  event_to,
  event_to_labels.l1_label AS event_to_label_type,
  event_to_labels.l2_label AS event_to_label_subtype,
  event_to_labels.project_name AS event_to_label,
  event_to_labels.address_name AS event_to_address_name,
  event_type,
  event_amount / power(10, COALESCE(adj.decimal_adjustment, 0)) AS event_amount,
  (
    event_amount / power(10, COALESCE(adj.decimal_adjustment, 0))
  ) * event_price.price AS event_amount_usd,
  event_currency
FROM
  {{ source(
    'near',
    'udm_events_near'
  ) }}
  e
  LEFT OUTER JOIN near_labels AS tx_from_labels
  ON e.tx_from = tx_from_labels.address
  LEFT OUTER JOIN near_labels AS tx_to_labels
  ON e.tx_to = tx_to_labels.address
  LEFT OUTER JOIN near_labels AS event_from_labels
  ON e.event_from = event_from_labels.address
  LEFT OUTER JOIN near_labels AS event_to_labels
  ON e.event_to = event_to_labels.address
  LEFT OUTER JOIN near_prices AS fee_price
  ON fee_price.hour = DATE_TRUNC(
    'hour',
    block_timestamp
  )
  AND fee_price.symbol = COALESCE(
    e.event_currency,
    'NEAR'
  )
  LEFT OUTER JOIN near_prices event_price
  ON event_price.hour = DATE_TRUNC(
    'hour',
    block_timestamp
  )
  AND event_price.symbol = e.event_currency
  LEFT OUTER JOIN near_decimals adj
  ON e.event_currency = adj.token_identifier
WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '3 days'
{% else %}
  block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
