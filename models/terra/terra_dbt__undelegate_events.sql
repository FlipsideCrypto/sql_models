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
    event_attributes:validator::string AS validator,
    event_attributes:completion_time::string AS completion_time,
    event_attributes:amount AS unbond_amount,
    event_attributes:"0_sender"::string AS "0_sender",
    event_attributes:"1_sender"::string AS "1_sender",
    event_attributes:"2_sender"::string AS "2_sender",
    event_attributes:action::string AS action,
    event_attributes:module::string AS module,
    event_attributes:"0_amount":amount AS event_transfer_0_amount,
    event_attributes:"0_amount":denom::string AS event_transfer_0_currency,
    event_attributes:"0_recipient"::string AS event_transfer_0_recipient,
    event_attributes:"0_sender"::string AS event_transfer_0_sender,
    event_attributes:"1_amount":amount AS event_transfer_1_amount,
    event_attributes:"1_amount":denom::string AS event_transfer_1_currency,
    event_attributes:"1_recipient"::string AS event_transfer_1_recipient,
    event_attributes:"1_sender"::string AS event_transfer_1_sender
  FROM {{source('terra', 'terra_msg_events')}}
  WHERE msg_module = 'staking' 
    AND msg_type = 'staking/MsgUndelegate'