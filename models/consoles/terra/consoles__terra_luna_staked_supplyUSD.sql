{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_luna_staked_supplyUSD']
) }}
--queryId: 320abd12-4abf-4c05-a64a-a541f49e8c5c

SELECT 
date, 
SUM(balance_usd) AS staked_supply_usd
FROM 
{{ ref('terra__daily_balances') }}
WHERE currency = 'LUNA' AND balance_type = 'staked'
GROUP BY 1 
ORDER BY 1 DESC 