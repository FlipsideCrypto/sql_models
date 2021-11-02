{{ 
  config(
    materialized='incremental', 
    sort=['date', 'currency'], 
    unique_key='date || address', 
    incremental_strategy='delete+insert',
    cluster_by=['date', 'address'],
    tags=['snowflake', 'terra', 'balances']
  )
}}

WITH prices AS (
 SELECT
   p.symbol,
   date_trunc('day', block_timestamp) as day,
   avg(price_usd) as price
 FROM
   {{ ref('terra__oracle_prices')}} p
 GROUP BY p.symbol, day
)
SELECT
  date,
  b.address,
  address_labels.l1_label as address_label_type,
  address_labels.l2_label as address_label_subtype,
  address_labels.project_name as address_label,
  address_labels.address as address_name,
  balance,
  balance * p.price as balance_usd,
  b.balance_type,
  currency
FROM
  {{source('terra', 'udm_daily_balances_terra')}} b
LEFT OUTER JOIN
  prices p
ON
  p.symbol = b.currency
  AND p.day = b.date
LEFT OUTER JOIN
  {{ref('silver_crosschain__address_labels')}} as address_labels
ON b.address = address_labels.address
WHERE 1=1
  {% if is_incremental() %}
    AND date >= getdate() - interval '3 days'
  -- {% else %}
  --   date >= getdate() - interval '12 months'
  {% endif %}