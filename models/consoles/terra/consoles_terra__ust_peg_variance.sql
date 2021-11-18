-- Velocity: 8dcd4c26-575b-4a9a-9b00-a70cf21bc4d7
{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', METRIC_DATE, currency)",
    tags = ['snowflake', 'terra', 'console']
) }}

select 
  date_trunc('day', block_timestamp) as metric_date,
  currency,
  symbol,
  avg(price_usd) as "AVG"
from {{ ref('terra__oracle_prices') }}
where block_timestamp::date > current_date - 180
  and symbol = 'UST'
  and price_usd > 0
group by metric_date,currency,symbol
order by metric_date desc 
