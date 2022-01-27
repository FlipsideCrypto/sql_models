{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', date, currency)",
  tags = ['snowflake', 'terra', 'console']
) }}

WITH ORACLE AS (

  SELECT
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DATE,
    currency,
    symbol,
    AVG(luna_exchange_rate) AS oracle_exchange,
    AVG(price_usd) AS oracle_usd
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    block_timestamp > getdate() - INTERVAL '6 month'
    AND symbol = 'SGT'
  GROUP BY
    DATE,
    currency,
    symbol
),
swaps AS (
  SELECT
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DATE,
    SUM(
      IFF(
        token_0_currency = 'LUNA',
        token_0_amount,
        token_1_amount
      )
    ) AS luna,
    SUM(
      IFF(
        token_0_currency = 'SGT',
        token_0_amount,
        token_1_amount
      )
    ) AS sgt,
    sgt / luna AS swap_exchange
  FROM
    {{ ref('terra__swaps') }}
  WHERE
    swap_pair IN (
      'SGT to LUNA',
      'LUNA to SGT'
    )
    AND block_timestamp > getdate() - INTERVAL '6 month'
  GROUP BY
    DATE
)
SELECT
  o.date,
  o.currency,
  o.symbol,
  o.oracle_exchange,
  s.swap_exchange,
  swap_exchange / oracle_exchange AS peg
FROM
  ORACLE o
  INNER JOIN swaps s
  ON (
    o.date = s.date
  )
ORDER BY
  DATE DESC
