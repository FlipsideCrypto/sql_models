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
  l.l1_label AS address_label_type,
  l.l2_label AS address_label_subtype,
  l.project_name AS address_label,
  l.address AS address_name,
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
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON b.address = l.address
  AND l.blockchain = 'terra'
WHERE
  1 = 1

{% if is_incremental() %}
AND DATE >= getdate() - INTERVAL '3 days'
{% endif %}
