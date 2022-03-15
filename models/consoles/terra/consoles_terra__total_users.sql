{{ config(
  materialized = 'view',
  unique_key = 'date',
  tags = ['snowflake', 'terra', 'console', 'terra_total_users']
) }}

SELECT 
  date,
  COUNT(DISTINCT address) as total_users
FROM {{ ref('terra__daily_balances') }}
WHERE date >= CURRENT_DATE - 30
GROUP BY 1