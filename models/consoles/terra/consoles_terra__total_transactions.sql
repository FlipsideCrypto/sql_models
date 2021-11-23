{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_total_transactions']
) }}
SELECT 
  DATE(block_timestamp) as day,
  COUNT(DISTINCT tx_id) as tx_count
FROM 
{{ ref('terra__msgs') }}
  WHERE day <= CURRENT_DATE
GROUP BY day
ORDER BY day DESC