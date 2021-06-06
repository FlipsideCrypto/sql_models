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
    event_attributes:amount:amount AS amount,
    event_attributes:amount:denom::string AS currency,
    event_attributes:recipient::string AS recipient,
    event_attributes:"0_sender"::string AS "0_sender",
    event_attributes:"1_sender"::string AS "1_sender",
    event_attributes:action::string AS action,
    event_attributes:module::string AS module
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'distribution' 
AND msg_type = 'distribution/MsgWithdrawValidatorCommission' 