{{ 
  config(
    materialized='view', 
    unique_key = "CONCAT_WS('-', date, addresses)",
    tags=['snowflake', 'console', 'terra', 'addresses_staking_LUNA']
  )
}}

WITH tmp AS(
  SELECT 
    date, 
    address, 
    balance
FROM {{ ref('terra__daily_balances') }}
WHERE balance_type = 'staked' and currency = 'LUNA'
AND date::date >= CURRENT_DATE - 60

)
  
SELECT 
  date, 
  count(distinct address) as addresses 
FROM tmp
WHERE balance > 0
group by date
ORDER BY date desc