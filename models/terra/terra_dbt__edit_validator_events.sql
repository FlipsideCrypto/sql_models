{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
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
  msg_index,
  event_type,
  event_attributes,
  event_attributes:action::string AS action,
  event_attributes:sender::string AS sender,
  event_attributes:module::string AS module,
  event_attributes:commission_rate::string AS commission_rate_attribute,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[0], ': ')[1]::float AS commission_rate,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[1], ': ')[1]::float AS commission_maxRate,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[2], ': ')[1]::float AS commission_maxChangeRate,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[3], ': ')[1]::string AS commission_updateTime,
  event_attributes:min_self_delegation AS min_self_delegation
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgEditValidator'
