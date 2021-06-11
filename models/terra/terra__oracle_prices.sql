{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'oracle']
  )
}}


WITH prices as (
SELECT 
  date_trunc('hour', recorded_at) as hour,
  symbol,
  avg(price) as price   
FROM {{ source('shared', 'prices')}}
WHERE asset_id = '4172'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
GROUP BY 1,2
),
luna_rate as (
SELECT 
  blockchain,
  chain_id,
  block_timestamp,
  block_id,
  REGEXP_REPLACE(attributes:denom::string,'\"','') as currency,
  attributes:exchange_rate as exchange_rate
FROM {{source('terra', 'terra_transitions')}}
WHERE event = 'exchange_rate_update' 
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
)

SELECT 
  blockchain,
  block_timestamp,
  currency,
  CASE 
   WHEN currency = 'usgd' THEN 'SGT'
   WHEN currency = 'uusd' THEN 'UST'
   WHEN currency = 'ukrw' THEN 'KRT'
   WHEN currency = 'unok' THEN 'NOT'
   WHEN currency = 'ucny' THEN 'CNT'
   WHEN currency = 'uinr' THEN 'INT'
   WHEN currency = 'ueur' THEN 'EUT'
   WHEN currency = 'udkk' THEN 'DKT'
   WHEN currency = 'uhkd' THEN 'HKT'
   WHEN currency = 'usek' THEN 'SET'
   WHEN currency = 'uthb' THEN 'THT'
   WHEN currency = 'umnt' THEN 'MNT'
   WHEN currency = 'ucad' THEN 'CAT'
   WHEN currency = 'ugbp' THEN 'GBT'
   WHEN currency = 'ujpy' THEN 'JPT'
   WHEN currency = 'usdr' THEN 'SDT'
   WHEN currency = 'uchf' THEN 'CHT'
   WHEN currency = 'uaud' THEN 'AUT'
   ELSE currency
  END as symbol,
  exchange_rate as luna_exchange_rate,
  price / exchange_rate as price_usd,
  price as luna_usd_price
FROM luna_rate

LEFT OUTER JOIN prices 
  ON date_trunc('hour', block_timestamp) = hour

UNION 

SELECT 
  'terra' as blockchain,
   hour as block_timestamp,
  'uluna' as currency,
  'LUNA' as symbol,
  1 as luna_exchange_rate,
  price as price_usd,
  price as luna_usd_price
FROM prices
  
