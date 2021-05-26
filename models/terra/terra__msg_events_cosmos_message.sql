{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events_cosmos_message']
  )
}}

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  tx_type,
  msg_module,
  msg_type, 
  event_type,
  event_attributes,
  event_attributes:action::string AS amount,
  event_attributes:module::string AS module,
  event_attributes:sender::string AS sender
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'cosmos'
AND event_type = 'message'