{{ 
  config(
    materialized='view', 
    unique_key='balance_date',  
    tags=['snowflake', 'console', 'terra', 'top_10_validators_by_rewards']
  )
}}

with prices as (
  select date_trunc('hour', block_timestamp) as date,
  symbol,
  currency,
  avg(price_usd) as price
  from {{ ref('terra__oracle_prices') }}
  where date >= CURRENT_DATE - 31
  group by 1,2,3
),
reward as (
  select 
  date_trunc('hour', t.block_timestamp) as date,
  sum(fl.value:amount / pow(10,6)) as event_amount,
  fl.value:denom::string as event_currency,
  event_attributes:validator::string as validator
from {{ ref('silver_terra__transitions') }} t
  , lateral flatten(input => event_attributes:amount) fl
where t.transition_type = 'begin_block'
  and t.event = 'rewards'
  and t.block_timestamp >= CURRENT_DATE - 30
group by 1,3,4)

select right(validator,10) AS validator,
round(sum(event_amount * price),2) as reward 
from reward r 
left outer join prices p 
on r.date = p.date 
and r.event_currency = p.currency
group by 1 
order by 2 desc
limit 10