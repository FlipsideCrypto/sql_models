{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', address, currency, avg_balance)",
  tags = ['snowflake', 'console', 'terra', 'top_holders']
) }}

with top_holders as (
SELECT * from (
SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM {{ ref('terra__daily_balances') }}
WHERE currency = 'KRT'
AND date >= current_date - 30
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000
)
  
UNION
 
(SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM {{ ref('terra__daily_balances') }}
WHERE currency = 'SDT'
AND date >= current_date - 30
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000)

UNION
  
(SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM {{ ref('terra__daily_balances') }}
WHERE currency = 'LUNA'
AND date >= current_date - 30
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000)
  
UNION

(SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM {{ ref('terra__daily_balances') }}
WHERE currency = 'UST'
AND date >= current_date - 30
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000)
)

SELECT * FROM top_holders