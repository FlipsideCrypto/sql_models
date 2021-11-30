{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', METRIC_DATE, currency)",
    tags = ['snowflake', 'terra', 'console']
) }}

with tmp as (
  select block_timestamp::date as block_date,
event_currency,
sum(event_amount_usd) as usd_volume
from {{ ref('terra__transfers') }}
WHERE event_currency IN('UST', 'KRT', 'SDT')
and block_timestamp::date >= CURRENT_DATE - 90
group by block_date,event_currency
),
tmp_2 as (
select block_date,
event_currency,
AVG(usd_volume) OVER (
    ORDER BY block_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) as avg_volume
from tmp
),
lb as (
select date,
currency,
sum(balance_usd) as usd_supply
from {{ ref('terra__daily_balances') }}
where date >= CURRENT_DATE - 90
and balance_type = 'liquid'
and balance > 0
and currency IN('UST', 'KRT', 'SDT')
group by date,currency
)

select t2.block_date as metric_date,
t2.event_currency as currency,
(avg_volume/usd_supply)*100 as value
from tmp_2 t2
left join lb on t2.block_date = lb.date
            and t2.event_currency = lb.currency
order by metric_date,currency