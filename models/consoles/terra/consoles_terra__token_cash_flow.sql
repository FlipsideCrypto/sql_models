{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', METRIC_DATE, EVENT_CURRENCY)",
    tags = ['snowflake', 'terra', 'console']
) }}

with tmp as (
select date,
currency,
sum(balance_usd) as staked_balance
from {{ ref('terra__daily_balances') }}
where date >= CURRENT_DATE - 60
and balance_type = 'staked'
and balance > 0
group by date,currency
order by date,currency
),
tmp_2 as (
select block_timestamp::date as metric_date,
currency as event_currency,
sum(event_amount_usd) as volume
from {{ ref('terra__reward') }}
where block_timestamp::date >= CURRENT_DATE - 60
and currency IN('KRT',
                'LUNA',
                'SDT',
                'UST')
group by metric_date,event_currency
)

select t2.metric_date,
t2.event_currency,
(t2.volume / t.staked_balance) * 100 as volume
from tmp_2 t2
left join tmp t on t2.metric_date = t.date
order by metric_date,event_currency