{{ 
  config(
    materialized='view', 
    unique_key='balance_date',  
    tags=['snowflake', 'console', 'terra', 'daily_rewards_USD']
  )
}}

WITH prices AS (
  SELECT date_trunc('hour', block_timestamp) as date,
  symbol,
  currency,
  avg(price_usd) as price
  FROM {{ ref('terra__oracle_prices') }}
  WHERE date >= CURRENT_DATE - 31
  GROUP BY date, symbol, currency
),

reward AS (
  SELECT 
    date_trunc('hour', t.block_timestamp) as date,
    sum(fl.value:amount / pow(10,6)) as event_amount,
    fl.value:denom::string as event_currency
  FROM {{ ref('silver_terra__transitions') }} t
    , lateral flatten(input => event_attributes:amount) fl
  WHERE t.transition_type = 'begin_block'
    AND t.event = 'rewards'
    AND t.block_timestamp >= CURRENT_DATE - 30
  GROUP BY date, event_currency)

SELECT 
  date_trunc('day', r.date) as day,
  sum(event_amount * price) as reward 
FROM reward r 
LEFT OUTER JOIN prices p 
  ON r.date = p.date 
  AND r.event_currency = p.currency
GROUP BY day 
ORDER BY day DESC