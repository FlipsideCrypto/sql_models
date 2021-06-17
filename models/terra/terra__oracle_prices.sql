{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='currency', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'oracle']
  )
}}

WITH prices as (
SELECT 
  date_trunc('hour', recorded_at) as block_timestamp,
  symbol as currency,
  avg(price) as price   
FROM {{ source('shared', 'prices_v2')}}
WHERE asset_id = '4172'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
GROUP BY 1,2
),
other_prices as (
SELECT 
  date_trunc('hour', recorded_at) as block_timestamp,
  symbol as currency,
  avg(price) as price   
FROM {{ source('shared', 'prices_v2')}}
WHERE asset_id IN('7857', '8857')
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
  REGEXP_REPLACE(event_attributes:denom::string,'\"','') as currency,
  event_attributes:exchange_rate as exchange_rate
FROM {{source('silver_terra', 'transitions')}}
WHERE event = 'exchange_rate_update' 
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
)

SELECT 
  blockchain,
  l.block_timestamp,
  l.currency,
  CASE 
   WHEN l.currency = 'usgd' THEN 'SGT'
   WHEN l.currency = 'uusd' THEN 'UST'
   WHEN l.currency = 'ukrw' THEN 'KRT'
   WHEN l.currency = 'unok' THEN 'NOT'
   WHEN l.currency = 'ucny' THEN 'CNT'
   WHEN l.currency = 'uinr' THEN 'INT'
   WHEN l.currency = 'ueur' THEN 'EUT'
   WHEN l.currency = 'udkk' THEN 'DKT'
   WHEN l.currency = 'uhkd' THEN 'HKT'
   WHEN l.currency = 'usek' THEN 'SET'
   WHEN l.currency = 'uthb' THEN 'THT'
   WHEN l.currency = 'umnt' THEN 'MNT'
   WHEN l.currency = 'ucad' THEN 'CAT'
   WHEN l.currency = 'ugbp' THEN 'GBT'
   WHEN l.currency = 'ujpy' THEN 'JPT'
   WHEN l.currency = 'usdr' THEN 'SDT'
   WHEN l.currency = 'uchf' THEN 'CHT'
   WHEN l.currency = 'uaud' THEN 'AUT'
   ELSE l.currency
  END as symbol,
  exchange_rate as luna_exchange_rate,
  price / exchange_rate as price_usd,
  'oracle' as source
FROM luna_rate l

LEFT OUTER JOIN prices p
  ON date_trunc('hour', l.block_timestamp) = p.block_timestamp

UNION 

SELECT 
  'terra' as blockchain,
   block_timestamp,
  'uluna' as currency,
  'LUNA' as symbol,
  1 as luna_exchange_rate,
  price as price_usd
  'coinmarketcap' as source
FROM prices

UNION 

SELECT 
  'terra' as blockchain,
   o.block_timestamp,
  o.currency as currency,
  o.currency as symbol,
  x.price / o.price as luna_exchange_rate,
  o.price as price_usd,
  'coinmarketcap' as source
FROM other_prices o
  
LEFT OUTER JOIN prices x
  ON date_trunc('hour', o.block_timestamp) = x.block_timestamp