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
    event_attributes:amount AS create_validator_amount,
    event_attributes:validator::string AS validator
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'staking' 
AND msg_type = 'staking/MsgCreateValidator'