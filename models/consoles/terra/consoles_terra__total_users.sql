{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'terra', 'console', 'terra_total_users']
) }}

SELECT 
  date,
  COUNT(DISTINCT address) as total_users
FROM {{ ref('terra__daily_balances') }}
WHERE date >= CURRENT_DATE - 30

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY 1