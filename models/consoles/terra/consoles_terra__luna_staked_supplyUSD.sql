{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_luna_staked_supplyUSD']
) }}

SELECT 
date, 
SUM(balance_usd) AS staked_supply_usd
FROM 
{{ ref('terra__daily_balances') }}
WHERE currency = 'LUNA' AND balance_type = 'staked'
and address <> 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
GROUP BY date
ORDER BY date DESC 