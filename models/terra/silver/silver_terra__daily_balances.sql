{{ config(
  materialized = 'incremental',
  unique_key = 'date || address || currency || balance_type',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'silver_terra', 'silver_terra__daily_balances']
) }}

with address_ranges as (
  select address,
  currency,
  balance_type,
  'terra' as blockchain,
  min(block_timestamp::date) as min_block_date,
  max(current_timestamp::date) as max_block_date
from {{ source('shared', 'terra_balances') }}
group by 1,2,3,4
),
CTE_MY_DATE AS (
  SELECT hour::date as date
  from {{ source('shared', 'hours') }}
  group by 1
  ),
  all_dates as (
  select c.date,
  a.address,
  a.currency,
  a.balance_type,
  a.blockchain
  from cte_my_date c
  left join address_ranges a on c.date between a.min_block_date and a.max_block_date
  where a.address is not null
  ),
  eth_balances as (
select address,
currency,
block_timestamp,
balance_type,
'terra' as blockchain,
balance
from {{ source('shared', 'terra_balances') }}
QUALIFY(row_number() over(partition by address, currency, block_timestamp::date, balance_type, blockchain order by block_timestamp desc)) = 1
),
balance_tmp as (
select d.date,
d.address,
d.currency,
b.balance,
d.balance_type,
d.blockchain
from all_dates d
left join eth_balances b on d.date = b.block_timestamp::date 
                        and d.address = b.address
                        and d.currency = b.currency
                        and d.balance_type = b.balance_type
                        and d.blockchain = b.blockchain
)

select date,
address,
currency,
balance_type,
blockchain,
last_value(balance ignore nulls) over(partition by address,currency,balance_type,blockchain order by date asc rows unbounded preceding) as balance
from balance_tmp