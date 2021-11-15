{{ 
  config(
    materialized='view', 
    unique_key='balance_date',  
    tags=['snowflake', 'console', 'terra', 'top_10_validators_by_rewards']
  )
}}

WITH prices AS (
  SELECT 
    date_trunc('hour', block_timestamp) as date,
    symbol,
    currency,
    avg(price_usd) as price
  FROM {{ ref('terra__oracle_prices') }}
  WHERE date >= CURRENT_DATE - 31
  GROUP BY date, symbol, currency
),

reward AS (
  SELECT 
    date_trunc('hour', t.block_timestamp) as day,
    sum(fl.value:amount / pow(10,6)) as event_amount,
    fl.value:denom::string as event_currency,
    event_attributes:validator::string as validator
  FROM {{ ref('silver_terra__transitions') }} t
    , lateral flatten(input => event_attributes:amount) fl
  WHERE t.transition_type = 'begin_block'
    AND t.event = 'rewards'
    AND t.block_timestamp >= CURRENT_DATE - 30
  GROUP BY day, event_currency, validator
)

SELECT 
  right(validator,10) AS validator,
  round(sum(event_amount * price),2) as reward 
FROM reward  
LEFT OUTER JOIN prices 
  ON day = date 
  AND event_currency = currency
GROUP BY validator 
ORDER BY reward DESC
LIMIT 10