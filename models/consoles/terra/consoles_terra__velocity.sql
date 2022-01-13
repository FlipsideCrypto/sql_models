{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', METRIC_DATE, currency)",
  tags = ['snowflake', 'terra', 'console']
) }}

WITH tmp AS (

  SELECT
    block_timestamp :: DATE AS block_date,
    event_currency,
    SUM(event_amount_usd) AS usd_volume
  FROM
    {{ ref('terra__transfers') }}
  WHERE
    event_currency IN(
      'UST',
      'KRT',
      'SDT'
    )
    AND block_timestamp :: DATE >= CURRENT_DATE - 90
  GROUP BY
    block_date,
    event_currency
),
tmp_2 AS (
  SELECT
    block_date,
    event_currency,
    AVG(usd_volume) over (
      ORDER BY
        block_date rows BETWEEN 29 preceding
        AND CURRENT ROW
    ) AS avg_volume
  FROM
    tmp
),
lb AS (
  SELECT
    DATE,
    currency,
    SUM(balance_usd) AS usd_supply
  FROM
    {{ ref('terra__daily_balances') }}
  WHERE
    DATE >= CURRENT_DATE - 90
    AND balance_type = 'liquid'
    AND balance > 0
    AND currency IN(
      'UST',
      'KRT',
      'SDT'
    )
  GROUP BY
    DATE,
    currency
)
SELECT
  t2.block_date AS metric_date,
  t2.event_currency AS currency,
  (
    avg_volume / usd_supply
  ) * 100 AS VALUE
FROM
  tmp_2 t2
  LEFT JOIN lb
  ON t2.block_date = lb.date
  AND t2.event_currency = lb.currency
  where metric_date <> current_date
ORDER BY
  metric_date,
  currency
