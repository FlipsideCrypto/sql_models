{{ config(
  materialized = 'view',
  unique_key = 'balance_date',
  tags = ['snowflake', 'console', 'terra', 'top_10_validators_by_rewards']
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
    ) AS DAY,
    SUM(fl.value :amount / pow(10, 6)) AS event_amount,
    fl.value :denom :: STRING AS event_currency,
    event_attributes :validator :: STRING AS validator
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
    DAY,
    event_currency,
    validator
)
SELECT
  RIGHT(
    validator,
    10
  ) AS validator,
  ROUND(SUM(event_amount * price), 2) AS reward
FROM
  reward
  LEFT OUTER JOIN prices
  ON DAY = DATE
  AND event_currency = currency
GROUP BY
  validator
ORDER BY
  reward DESC
LIMIT
  10
