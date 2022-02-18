{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, date, address, metric_slug)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'gold', 'near', 'gold__near_reward_flow', 'address_labels'],
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
      'day',
      recorded_at
    ) AS DAY,
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
    DAY
)
SELECT
  'near' AS blockchain,
  xfer_date AS DATE,
  entity_id AS address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address_name AS address_name,
  metric_slug,
  metric_value,
  metric_value * p.price AS metric_value_usd
FROM
  {{ source(
    'near',
    'near_daily_reward_flow'
  ) }}
  f
  LEFT OUTER JOIN near_labels AS address_labels
  ON f.entity_id = address_labels.address
  LEFT OUTER JOIN near_prices p
  ON p.day = xfer_date
  AND p.symbol = 'NEAR'
WHERE

{% if is_incremental() %}
xfer_date >= getdate() - INTERVAL '3 days'
{% else %}
  xfer_date >= getdate() - INTERVAL '9 months'
{% endif %}
