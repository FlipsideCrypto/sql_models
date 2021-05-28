{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'swap']
  )
}}

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type,
  REGEXP_REPLACE(msg_value:trader,'\"','') as trader,
  REGEXP_REPLACE(msg_value:ask_denom,'\"','') as ask_currency,
  REGEXP_REPLACE(msg_value:offer_coin:amount,'\"','') as event_amount,
  REGEXP_REPLACE(msg_value:offer_coin:denom,'\"','') as offer_currency
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'market' 
  AND msg_type = 'market/MsgSwap'