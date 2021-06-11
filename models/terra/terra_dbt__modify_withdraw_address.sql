{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'reward']
  )
}}

WITH rewards_event AS (
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
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),

rewards AS (
  SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
  REGEXP_REPLACE(msg_value:withdraw_address,'\"','') as withdraw_address
FROM {{source('terra', 'terra_msgs')}} 
WHERE msg_module = 'distribution' 
  AND msg_type = 'distribution/MsgModifyWithdrawAddress' 
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),

rewards_event_base AS (
  SELECT DISTINCT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tx_id, 
    msg_type
  FROM rewards 
), 

message AS (
  SELECT
      tx_id, 
      action,
      module,
      sender
  FROM rewards_event 
  WHERE event_type = 'message' AND msg_index = 0
)


SELECT
  rewards_event_base.blockchain,
  rewards_event_base.chain_id,
  rewards_event_base.tx_status,
  rewards_event_base.block_id,
  rewards_event_base.block_timestamp, 
  rewards_event_base.tx_id, 
  rewards_event_base.msg_type, 
  action,
  module,
  sender,
  rewards.withdraw_address
FROM rewards_event_base
LEFT JOIN message
ON rewards_event_base.tx_id = message.tx_id
LEFT JOIN rewards
ON rewards_event_base.tx_id = rewards.tx_id