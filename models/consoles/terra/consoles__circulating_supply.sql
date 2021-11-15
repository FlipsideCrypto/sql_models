{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', metric_date, currency)",
  tags = ['snowflake', 'terra', 'balances', 'console', 'circulating_supply']
) }}

SELECT 
       DATE_TRUNC('day', date) AS metric_date,
       currency,
       SUM(balance) AS total_balance
FROM  {{ ref('terra__daily_balances') }}
WHERE currency IN('KRT',
                'LUNA',
                'SDT',
                'UST')
GROUP BY metric_date, currency
ORDER BY metric_date DESC
