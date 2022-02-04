{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', address, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['address'],
  tags = ['snowflake', 'console', 'terra', 'top_holders']
) }}

WITH recent_balances AS (
  SELECT 
  date,
  address,
  currency,
  balance
  FROM {{ ref('terra__daily_balances') }}
  WHERE currency IN ('KRT', 'SDT', 'LUNA', 'UST')
  AND date >= current_date - 30

  {% if is_incremental() %}
  AND DATE >= getdate() - INTERVAL '1 days'
  {% endif %}
),

top_holders as (
SELECT * from (
SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM recent_balances
WHERE currency = 'KRT'
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000
)
  
UNION
 
(SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM recent_balances
WHERE currency = 'SDT'
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000)

UNION
  
(SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM recent_balances
WHERE currency = 'LUNA'
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000)
  
UNION

(SELECT 
address,
currency,
AVG(balance) as avg_balance
FROM recent_balances
WHERE currency = 'UST'
GROUP BY address, currency
ORDER BY avg_balance desc
LIMIT 1000)
)

SELECT * FROM top_holders