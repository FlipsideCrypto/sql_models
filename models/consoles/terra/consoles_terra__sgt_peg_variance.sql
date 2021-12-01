{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', date, currency)",
    tags = ['snowflake', 'terra', 'console']
) }}

with oracle as (
select 
  date_trunc('day', block_timestamp) as date,
  currency,
  symbol,
  avg(luna_exchange_rate) as oracle_exchange,
  avg(price_usd) as oracle_usd
from {{ ref('terra__oracle_prices') }}
where block_timestamp > getdate() - interval '6 month'
  and symbol = 'SGT'
group by date, currency, symbol
),

swaps as (
SELECT
  date_trunc('day', block_timestamp) as date,
  sum(iff(token_0_currency = 'LUNA', token_0_amount, token_1_amount)) as LUNA,
  sum(iff(token_0_currency = 'SGT', token_0_amount, token_1_amount)) as SGT,
  SGT / LUNA as swap_exchange
from {{ ref('terra__swaps') }}
where swap_pair in ('SGT to LUNA', 'LUNA to SGT')
and block_timestamp > getdate() - interval '6 month'
group by date
)

select 
  o.date,
  o.currency,
  o.symbol,
  o.oracle_exchange,
  s.swap_exchange,
  swap_exchange / oracle_exchange as peg
from oracle o inner join swaps s on (o.date = s.date)
order by date desc