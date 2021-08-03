{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_timestamp', 
    incremental_strategy='delete+insert',
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
GROUP BY 1,2
),

other_prices as (
SELECT 
  date_trunc('hour', recorded_at) as block_timestamp,
  symbol as currency,
  avg(price) as price   
FROM {{ source('shared', 'prices_v2')}}
WHERE asset_id IN('7857', '8857')
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

),

massets as(
SELECT 
  m.blockchain,
  m.chain_id,
  m.block_timestamp,
  m.block_id,
  m.msg_value:execute_msg:feed_price:prices[0][0]::string as currency,
  p.address_name as symbol,
  m.msg_value:execute_msg:feed_price:prices[0][1] as price
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} p 
  ON msg_value:execute_msg:feed_price:prices[0][0]::string = p.address 

WHERE msg_value:contract = 'terra1t6xe0txzywdg85n6k8c960cuwgh6l8esw6lau9' --Mirror Oracle Feeder
  AND msg_value:sender = 'terra128968w0r6cche4pmf4xn5358kx2gth6tr3n0qs' -- Make sure we are pulling right events
    
  {% if is_incremental() %}
    AND m.block_timestamp >= getdate() - interval '1 days'
  -- {% else %}
  --   AND m.block_timestamp >= getdate() - interval '9 months'
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
  price as price_usd,
  'coinmarketcap' as source
FROM prices

UNION 

SELECT 
  'terra' as blockchain,
   o.block_timestamp,
  CASE 
    WHEN o.currency = 'MIR' THEN 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6' 
    WHEN o.currency = 'ANC' THEN 'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' 
    ELSE NULL 
  END AS currency,
  o.currency as symbol,
  x.price / o.price as luna_exchange_rate,
  o.price as price_usd,
  'coinmarketcap' as source
FROM other_prices o
  
LEFT OUTER JOIN prices x
  ON date_trunc('hour', o.block_timestamp) = x.block_timestamp

UNION

SELECT 
  ma.blockchain,
  ma.block_timestamp,
  ma.currency as currency,
  ma.symbol as symbol,
  pp.price / ma.price as luna_exchange_rate,
  ma.price as price_usd,
  'oracle' as source
FROM massets ma

LEFT OUTER JOIN prices pp
  ON date_trunc('hour', ma.block_timestamp) = pp.block_timestamp