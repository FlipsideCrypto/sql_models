{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', date, address, balance_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date', 'currency'],
  tags = ['snowflake', 'gold', 'near', 'gold__near_daily_balances', 'address_labels'],
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
  DATE,
  b.address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address_name AS address_address_name,
  balance / power(10, COALESCE(adj.decimal_adjustment, 0)) AS balance,
  balance / power(10, COALESCE(adj.decimal_adjustment, 0)) * p.price AS balance_usd,
  balance_type,
  currency
FROM
  {{ ref('silver_near__daily_balances') }}
  b
  LEFT OUTER JOIN near_decimals adj
  ON b.currency = adj.token_identifier
  LEFT OUTER JOIN near_prices p
  ON p.symbol = b.currency
  AND p.day = b.date
  LEFT OUTER JOIN near_labels AS address_labels
  ON b.address = address_labels.address
WHERE

{% if is_incremental() %}
DATE >= getdate() - INTERVAL '3 days'
{% else %}
  DATE >= getdate() - INTERVAL '9 months'
{% endif %}
