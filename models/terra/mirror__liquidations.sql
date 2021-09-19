{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'mirror', 'liquidations']
  )
}}

WITH prices AS (

  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price
    FROM {{ ref('terra__oracle_prices')}} 
    GROUP BY 1,2,3

), 

msgs AS (

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:msg:auction:position_idx as collateral_id,
  msg_value:execute_msg:send:contract::string as contract_address
FROM terra.msgs 
WHERE msg_value:execute_msg:send:msg:auction IS NOT NULL 
  AND tx_status = 'SUCCEEDED'
),

events AS (

SELECT 
  tx_id,
  COALESCE((event_attributes:"0_tax_amount"[0]:amount + event_attributes:"1_tax_amount"[0]:amount), event_attributes:tax_amount[0]:amount) / POW(10,6) AS tax,
  COALESCE(event_attributes:"1_tax_amount"[0]:denom::string,event_attributes:tax_amount[0]:denom::string) AS tax_currency,
  
  event_attributes:protocol_fee[0]:amount / POW(10,6) as protocol_fee,
  event_attributes:protocol_fee[0]:denom::string as protocol_fee_currency,
  
  event_attributes:liquidated_amount[0]:amount / POW(10,6) as liquidated_amount,
  event_attributes:liquidated_amount[0]:denom::string as liquidated_currency,
  
  event_attributes:return_collateral_amount[0]:amount / POW(10,6) as return_collateral_amount,
  event_attributes:return_collateral_amount[0]:denom::string as return_collateral_currency,
  
  event_attributes:unlocked_amount[0]:amount / POW(10,6) as unlocked_amount,
  event_attributes:unlocked_amount[0]:denom::string as unlocked_curency,
  
  event_attributes:owner::string as owner
FROM terra.msg_events 
WHERE event_type = 'from_contract'
  AND tx_id IN(SELECT tx_id FROM msgs)
  AND event_attributes:return_collateral_amount IS NOT NULL
  AND tx_status = 'SUCCEEDED'

)

SELECT 
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender as buyer,
  owner,
  tax,
  tax * t.price as tax_usd,
  tax_currency,
  protocol_fee,
  protocol_fee * p.price as protocol_fee_usd,
  protocol_fee_currency,
  liquidated_amount,
  liquidated_amount * l.price as liquidated_amount_usd,
  liquidated_currency,
  return_collateral_amount,
  return_collateral_amount * r.price as return_collateral_amount_usd,
  return_collateral_currency,
  unlocked_amount,
  unlocked_amount * u.price as unlocked_amount_usd,
  unlocked_curency,
  collateral_id,
  contract_address,
  g.address_name AS contract_label
FROM msgs m

JOIN events e 
  ON m.tx_id = e.tx_id

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as g
ON m.contract_address = g.address

LEFT OUTER JOIN prices t
 ON date_trunc('hour', m.block_timestamp) = t.hour
 AND tax_currency = t.currency 

LEFT OUTER JOIN prices p
 ON date_trunc('hour', m.block_timestamp) = p.hour
 AND protocol_fee_currency = p.currency 

LEFT OUTER JOIN prices l
 ON date_trunc('hour', m.block_timestamp) = l.hour
 AND liquidated_currency = l.currency 

LEFT OUTER JOIN prices r
 ON date_trunc('hour', m.block_timestamp) = r.hour
 AND return_collateral_currency = r.currency 

LEFT OUTER JOIN prices u
 ON date_trunc('hour', m.block_timestamp) = u.hour
 AND unlocked_curency = u.currency 