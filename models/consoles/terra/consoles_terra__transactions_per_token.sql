{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_transactions_per_token']
) }}
SELECT 
block_timestamp::date AS metric_date,
event_currency as currency,
count(distinct tx_id) AS metric_value
FROM
{{ ref('terra__transfers') }}
WHERE event_currency IN('KRT',
                'LUNA',
                'SDT',
                'UST')
and block_timestamp::date >= CURRENT_DATE - 180
GROUP BY metric_date, currency
ORDER BY metric_date DESC, currency DESC