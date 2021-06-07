{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'swap']
  )
}}

WITH msgs AS(
SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp, 
  tx_status,
  tx_id, 
  msg_type,
  REGEXP_REPLACE(msg_value:trader,'\"','') as trader,
  REGEXP_REPLACE(msg_value:ask_denom,'\"','') as ask_currency,
  REGEXP_REPLACE(msg_value:offer_coin:amount,'\"','') as offer_amount,
  REGEXP_REPLACE(msg_value:offer_coin:denom,'\"','') as offer_currency,
  msg_value
FROM flipside_prod_db.silver.terra_msgs 
WHERE msg_module = 'market' 
  AND msg_type = 'market/MsgSwap' 
),

events_transfer as(  
SELECT tx_id,
       event_type,
       event_attributes,
       REGEXP_REPLACE(event_attributes:"0_sender",'\"','') as "0_sender",
       REGEXP_REPLACE(event_attributes:"0_recipient",'\"','') as "0_recipient",
       REGEXP_REPLACE(event_attributes:"0_amount":amount,'\"','') as "0_amount",
       REGEXP_REPLACE(event_attributes:"0_amount":denom,'\"','') as "0_amount_currency",
       REGEXP_REPLACE(event_attributes:"1_sender",'\"','') as "1_sender",
       REGEXP_REPLACE(event_attributes:"1_recipient",'\"','') as "1_recipient",
       REGEXP_REPLACE(event_attributes:"1_amount":amount,'\"','') as "1_amount",
       REGEXP_REPLACE(event_attributes:"1_amount":denom,'\"','') as "1_amount_currency",
       event_attributes as transfer_attr
FROM flipside_prod_db.silver.terra_msg_events
WHERE event_type = 'transfer'
  AND msg_type = 'market/MsgSwap'
),

fees as(
select 
  tx_id,
  event_type,
  event_attributes,
  REGEXP_SUBSTR(event_attributes:swap_fee, '[0-9]+\.[0-9]+') as swap_fee_amount,
  REGEXP_REPLACE(event_attributes:swap_fee, '[^a-zA-Z]', '')as swap_fee_currency,
  event_attributes as fee_attr
FROM flipside_prod_db.silver.terra_msg_events
WHERE event_type = 'swap'
  AND msg_type = 'market/MsgSwap'
),

contract as (
select 
  tx_id,
  event_type,
  event_attributes,
  REGEXP_REPLACE(event_attributes:contract_address,'\"','') as contract_address,
  event_attributes as contract_attr
FROM flipside_prod_db.silver.terra_msg_events
WHERE event_type = 'execute_contract'
  AND msg_type = 'wasm/MsgExecuteContract'
)



SELECT 
  m.blockchain,
  m.chain_id,
  m.block_id,
  m.block_timestamp, 
  m.tx_status,
  m.tx_id, 
  f.swap_fee_amount,
  f.swap_fee_currency,
  m.trader,
  m.ask_currency,
  m.offer_amount,
  m.offer_currency,
  et."0_sender",
  et."0_recipient",
  et."0_amount",
  et."0_amount_currency",
  et."1_sender",
  et."1_recipient",
  et."1_amount",
  et."1_amount_currency",
  c.contract_address,
  msg_value,
  transfer_attr,
  fee_attr,
  contract_attr
FROM msgs m

LEFT OUTER JOIN events_transfer et 
 ON m.tx_id = et.tx_id 

LEFT OUTER JOIN fees f 
 ON m.tx_id = f.tx_id 
 
LEFT OUTER JOIN contract c
 ON m.tx_id = c.tx_id 