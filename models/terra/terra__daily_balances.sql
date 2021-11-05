{{ config(
  materialized = 'incremental',
  sort = ['date', 'currency'],
  unique_key = "CONCAT_WS('-', date, address)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'terra', 'balances']
) }}

WITH prices AS (

  SELECT
    p.symbol,
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
    p
  GROUP BY
    p.symbol,
    DAY
)
SELECT
  DATE,
  b.address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address AS address_name,
  balance,
  balance * p.price AS balance_usd,
  b.balance_type,
  currency
FROM
  {{ ref('silver_terra__daily_balances') }}
  b
  LEFT OUTER JOIN prices p
  ON p.symbol = b.currency
  AND p.day = b.date
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS address_labels
  ON b.address = address_labels.address
WHERE
  1 = 1

{% if is_incremental() %}
AND DATE >= getdate() - INTERVAL '3 days' -- {% else %}
--   date >= getdate() - interval '12 months'
{% endif %}
