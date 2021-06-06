{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'reward']
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
    event_attributes:module::string AS module,
    event_attributes:sender::string AS sender,
    event_attributes:withdraw_address::string AS withdraw_address
FROM {{source('terra', 'terra_msg_events')}} 
WHERE msg_module = 'distribution' 
AND msg_type = 'distribution/MsgModifyWithdrawAddress' 
