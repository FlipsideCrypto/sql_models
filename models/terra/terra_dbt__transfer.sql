{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'transfer']
  )
}}

-- Batched Transfers
WITH inputs as(
SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,  
  tx_id,
  msg_type, 
  REGEXP_REPLACE(vm.value:address,'\"','') as event_from,
  ve.value:amount as event_amount,
  REGEXP_REPLACE(ve.value:denom,'\"','') as event_currency
FROM {{source('terra', 'terra_msgs')}}
, lateral flatten(input => msg_value:inputs) vm
, lateral flatten(input => vm.value:coins) ve
WHERE msg_module = 'bank'
  AND msg_type = 'bank/MsgMultiSend'),

outputs as(
SELECT 
  blockchain,
  chain_id,
  block_id,
  tx_status,
  block_timestamp,  
  tx_id,
  tx_type,
  REGEXP_REPLACE(vm.value:address,'\"','') as event_to,
  ve.value:amount/ pow(10,6) as event_amount,
  REGEXP_REPLACE(ve.value:denom,'\"','') as event_currency
FROM {{source('terra', 'terra_msgs')}}
, lateral flatten(input => msg_value:outputs) vm
, lateral flatten(input => vm.value:coins) ve
WHERE msg_module = 'bank'
  AND msg_type = 'bank/MsgMultiSend')
  
SELECT 
  i.blockchain,
  i.chain_id,
  i.block_id,
  i.tx_status,
  i.block_timestamp,  
  i.tx_id,
  i.event_from,
  o.event_to,
  i.event_amount,
  i.event_currency
FROM inputs i

JOIN outputs o 
  ON i.tx_id = o.tx_id
  AND i.event_amount = o.event_amount
  AND i.event_currency = o.event_currency

UNION

-- Simple Transfers

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type, 
  REGEXP_REPLACE(msg_value:from_address,'\"','') as event_from,
  REGEXP_REPLACE(msg_value:to_address,'\"','') as event_to,
  msg_value:amount[0]:amount / pow(10,6) as event_amount,
  REGEXP_REPLACE(msg_value:amount[0]:denom,'\"','') as event_currency
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'bank'
  AND msg_type = 'bank/MsgSend'