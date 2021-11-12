{{ 
  config(
    materialized='view', 
    unique_key = "CONCAT_WS('-', date, address)",
    tags=['snowflake', 'console', 'terra', 'addresses_staking_LUNA']
  )
}}

WITH tmp AS(
  SELECT 
    date, 
    address, 
    balance
FROM "FLIPSIDE_DEV_DB"."SILVER_TERRA"."DAILY_BALANCES"
WHERE balance_type = 'staked' and currency = 'LUNA'
AND date::date >= CURRENT_DATE - 60

)
  
SELECT 
  date, 
  address, 
  balance
FROM tmp
WHERE balance > 0
ORDER BY 1, 2