{{ config(
  materialized = 'view',
  unique_key = 'balance_date',
  tags = ['snowflake', 'console', 'terra', 'daily_rewards_USD']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS DATE,
    symbol,
    currency,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    DATE >= CURRENT_DATE - 31
  GROUP BY
    DATE,
    symbol,
    currency
),
reward AS (
  SELECT
    DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) AS DATE,
    SUM(fl.value :amount / pow(10, 6)) AS event_amount,
    fl.value :denom :: STRING AS event_currency
  FROM
    {{ ref('silver_terra__transitions') }}
    t,
    LATERAL FLATTEN(
      input => event_attributes :amount
    ) fl
  WHERE
    t.transition_type = 'begin_block'
    AND t.event = 'rewards'
    AND t.block_timestamp >= CURRENT_DATE - 30
  GROUP BY
    DATE,
    event_currency
)
SELECT
  DATE_TRUNC(
    'day',
    r.date
  ) AS DAY,
  SUM(
    event_amount * price
  ) AS reward
FROM
  reward r
  LEFT OUTER JOIN prices p
  ON r.date = p.date
  AND r.event_currency = p.currency
GROUP BY
  DAY
ORDER BY
  DAY DESC
