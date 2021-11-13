{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_fees_UST']
) }}
--queryId: 0c56d097-8b37-49b3-8014-40121d5f75ee
with rawfees as (
  SELECT 
  block_timestamp::date as metric_date,
  case when chain_id = 'columbus-5' then UPPER(SUBSTRING(fee[0]:amount[0]:denom::string, 2, 2)) 
       else UPPER(SUBSTRING(fee[0]:denom::string, 2, 2)) end as fee_denom,
  sum(case when chain_id = 'columbus-5' then fee[0]:amount[0]:amount
       else fee[0]:amount end)/POW(10,6)  as amount
FROM 
{{ ref('terra__transactions') }}
WHERE fee_denom IS NOT NULL 
  --AND fee_denom in ('UST', 'SDT', 'AUT', 'CAT', 'EUT', 'JPT', 'KRT', 'LUNA', 'MNT')
  AND block_timestamp::date >= CURRENT_DATE - 30
GROUP BY metric_date, fee_denom
ORDER BY 1,2
),

prices as (
  select
  block_timestamp::date as metric_date,
  symbol,
  SUBSTRING(symbol,1,2) as fee_denom,
  avg(price_usd) as price
  from 
  {{ ref('terra__oracle_prices') }}
  where block_timestamp::date > CURRENT_DATE - 30
  and symbol in ('UST', 'SDT', 'AUT', 'CAT', 'EUT', 'JPT', 'KRT', 'LUNA', 'MNT')
  group by 1, 2, 3
)

select 
r.metric_date as BLOCK_DATE,
symbol as currency,
amount * price as fee
from rawfees r join prices p 
on r.metric_date = p.metric_date 
and r.fee_denom = p.fee_denom
order by 1 desc, 2