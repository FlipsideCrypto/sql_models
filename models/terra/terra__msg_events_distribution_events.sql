{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events_distribution']
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
  event_attributes:amount:amount AS amount,
  event_attributes:amount:denom::string AS currency,
  event_attributes:validator::string AS validator
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'distribution'
AND event_type = 'withdraw_rewards'