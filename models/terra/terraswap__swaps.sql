{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'terraswap', 'swap']
  )
}}


WITH prices AS (

  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price_usd
    FROM {{ ref('terra__oracle_prices')}} 
    GROUP BY 1,2,3

), 

msgs as (
-- native to non-native/native
SELECT
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:contract::string as pool_address
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_value:execute_msg:swap IS NOT NULL
  AND tx_status = 'SUCCEEDED'
  

 UNION 
 
-- non-native to native
SELECT
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:contract::string as pool_address
FROM {{source('silver_terra', 'msgs')}}

WHERE msg_value:execute_msg:send:msg:swap IS NOT NULL
  AND tx_status = 'SUCCEEDED'
),

events as (
SELECT
  tx_id,
  as_number(event_attributes:tax_amount)/POW(10,6) as tax_amount,
  event_attributes:commission_amount::numeric/POW(10,6) as commission_amount,
  event_attributes:offer_amount::numeric/POW(10,6) as offer_amount,
  event_attributes:offer_asset::string as offer_currency,
  event_attributes:return_amount::numeric/POW(10,6) as return_amount,
  event_attributes:ask_asset::string as return_currency
FROM {{source('silver_terra', 'msg_events')}}

WHERE event_type = 'from_contract'
  AND tx_id IN (SELECT DISTINCT tx_id 
 	  		    FROM msgs)
)


SELECT 
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  tax_amount,
  commission_amount,
  offer_amount,
  offer_amount * o.price AS offer_amount_usd,
  offer_currency,
  return_amount,
  return_amount * r.price AS return_amount_usd,
  return_currency,
  pool_address,
  l.address_name AS pool_name
FROM msgs m 

JOIN events e 
  ON m.tx_id = e.tx_id

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND m.offer_currency = o.currency 

LEFT OUTER JOIN prices r
 ON date_trunc('hour', block_timestamp) = r.hour
 AND m.return_currency = r.currency  

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} l
  ON pool_address = l.address