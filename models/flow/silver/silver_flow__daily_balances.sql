{{ config(
  materialized = 'incremental',
  unique_key = 'date || address || currency || balance_type',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_flow', 'silver_flow__daily_balances']
) }}

with address_ranges as (
  select address,
  min(block_timestamp::date) as min_block_date,
  max(current_timestamp::date) as max_block_date
from {{ source('shared', 'flow_balances') }}
group by 1
),
CTE_MY_DATE AS (
  SELECT hour::date as date
  from {{ source('shared', 'hours') }}
  group by 1
  ),
  all_dates as (
  select c.date,
  a.address
  from cte_my_date c
  left join address_ranges a on c.date between a.min_block_date and a.max_block_date
  where a.address is not null
  ),
  eth_balances as (
select *
from {{ source('shared', 'flow_balances') }}
QUALIFY(row_number() over(partition by address, currency, block_timestamp::date order by block_timestamp desc)) = 1
),
balance_tmp as (
select date,
d.address,
currency,
balance,
balance_type,
blockchain
from all_dates d
left join eth_balances b on d.date = b.block_timestamp::date 
                        and d.address = b.address
)

select date,
address,
last_value(currency ignore nulls) over(partition by address order by date asc rows unbounded preceding) as currency,
last_value(balance ignore nulls) over(partition by address order by date asc rows unbounded preceding) as balance,
last_value(balance_type ignore nulls) over(partition by address order by date asc rows unbounded preceding) as balance_type,
last_value(blockchain ignore nulls) over(partition by address order by date asc rows unbounded preceding) as blockchain
from balance_tmp