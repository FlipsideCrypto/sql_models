{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
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
    event_attributes:amount[0]:amount / POW(10,6) AS event_rewards_amount,
    event_attributes:amount[0]:denom::string AS event_rewards_currency,
    event_attributes:validator::string AS validator,
    event_attributes:amount[0]:amount / POW(10,6) AS event_transfer_amount,
    event_attributes:amount[0]:denom::string AS event_transfer_currency,
    event_attributes:sender::string AS sender,
    event_attributes:recipient::string AS recipient,
    event_attributes:action::string AS action
  FROM {{source('silver_terra', 'msg_events')}} 
  WHERE msg_module = 'distribution'
  AND msg_type = 'distribution/MsgWithdrawDelegationReward'
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
    msg_index,
    REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
    REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address,
    REGEXP_REPLACE(msg_value:amount:amount / POW(10,6),'\"','') as event_amount,
    REGEXP_REPLACE(msg_value:amount:denom,'\"','') as event_currency
  FROM {{source('silver_terra', 'msgs')}} 
  WHERE msg_module = 'distribution' 
    AND msg_type = 'distribution/MsgWithdrawDelegationReward'
), 

rewards_event_base AS (
  SELECT DISTINCT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tx_id, 
    msg_type, 
    msg_index
  FROM rewards 
), 

transfer AS (
  SELECT
      tx_id, 
      event_transfer_amount,
      event_transfer_currency,
      sender,
      recipient
  FROM rewards_event 
  WHERE event_type = 'transfer' AND msg_index = 0
),
message AS (
  SELECT
    tx_id,
    action
  FROM rewards_event 
  WHERE event_type = 'message' AND msg_index = 0
),
withdraw_rewards AS (
  SELECT
    tx_id,
    msg_index,
    event_rewards_amount,
    event_rewards_currency,
    validator
  FROM rewards_event 
  WHERE event_type = 'withdraw_rewards' 
)


SELECT
  rewards_event_base.blockchain,
  rewards_event_base.chain_id,
  rewards_event_base.tx_status,
  rewards_event_base.block_id,
  rewards_event_base.block_timestamp, 
  rewards_event_base.tx_id, 
  rewards_event_base.msg_type, 
  rewards_event_base.msg_index,
  event_transfer_amount,
  event_transfer_currency,
  sender,
  recipient,
  action,
  event_rewards_amount,
  event_rewards_currency,
  validator,
  rewards.delegator_address AS delegator
FROM rewards_event_base
LEFT JOIN transfer
ON rewards_event_base.tx_id = transfer.tx_id
LEFT JOIN message
ON rewards_event_base.tx_id = message.tx_id
LEFT JOIN withdraw_rewards
ON rewards_event_base.tx_id = withdraw_rewards.tx_id AND rewards_event_base.msg_index = withdraw_rewards.msg_index
LEFT JOIN rewards
ON rewards_event_base.tx_id = rewards.tx_id AND rewards_event_base.msg_index = rewards.msg_index
