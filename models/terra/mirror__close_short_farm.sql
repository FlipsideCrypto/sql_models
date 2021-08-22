{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'mirror', 'short_farm']
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

tx AS (

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value
FROM terra.msgs
WHERE msg_value:execute_msg:send:msg:burn IS NOT NULL
  AND msg_value:execute_msg:send:contract::string = 'terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj'
  AND tx_status = = 'SUCCEEDED'

),

event_tx AS (

SELECT 
  block_timestamp,
  tx_id,
  event_attributes
FROM terra.msg_events 
WHERE tx_id IN(select tx_id from tx)
  AND event_type = 'from_contract'

),

msgs as (

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp, 
  tx_id,
  msg_value:execute_msg:send:msg:burn:position_idx as collateral_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:contract::string as contract_address,
  l.address_name AS contract_label
FROM tx

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON contract_address = l.address

),

withdraw_events AS (

SELECT 
  tx_id,
  
  event_attributes:withdraw_amount[0]:amount / POW(10,6) AS withdraw_amount,
  withdraw_amount * o.price AS withdraw_amount_usd,
  event_attributes:withdraw_amount[0]:denom::string AS withdraw_currency,
  
  event_attributes:unlocked_amount[0]:amount / POW(10,6) AS unlocked_amount,
  unlocked_amount * i.price AS unlocked_amount_usd,
  event_attributes:unlocked_amount[0]:denom::string AS unlocked_currency,
  
  (event_attributes:"0_tax_amount"[0]:amount + event_attributes:"1_tax_amount"[0]:amount) / POW(10,6) AS tax,
  tax * a.price AS tax_usd,
  event_attributes:"0_tax_amount"[0]:denom::string AS tax_currency
  
FROM event_tx t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.withdraw_currency = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.unlocked_currency = i.currency  

LEFT OUTER JOIN prices a
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.tax_currency = i.currency  

WHERE event_attributes:withdraw_amount IS NOT NULL 

),

burn_events AS (

SELECT 
  tx_id,
  
  event_attributes:burn_amount[0]:amount / POW(10,6) AS burn_amount,
  burn_amount * o.price AS burn_amount_usd,
  event_attributes:burn_amount[0]:denom::string AS burn_currency,  
  
  event_attributes:protocol_fee[0]:amount / POW(10,6) AS protocol_fee,
  protocol_fee * i.price AS protocol_fee_usd,
  event_attributes:protocol_fee[0]:denom::string AS protocol_fee_currency
FROM event_tx t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.burn_currency = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.protocol_fee_currency = i.currency  

WHERE event_attributes:burn_amount IS NOT NULL
  
)

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  collateral_id,
  sender,
  withdraw_amount,
  withdraw_amount_usd,
  withdraw_currency,
  burn_amount,
  burn_amount_usd,
  burn_currency,
  unlocked_amount,
  unlocked_amount_usd,
  unlocked_currency,
  tax,
  tax_usd,
  tax_currency,
  protocol_fee,
  protocol_fee_usd,
  protocol_fee_currency,
  contract_address,
  contract_label
FROM msgs m

JOIN withdraw_events w 
  ON m.tx_id = w.tx_id

JOIN burn_events b 
  ON m.tx_id = b.tx_id

WHERE unlocked_amount IS NOT NULL