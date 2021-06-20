{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'reward_test']
  )
}}

WITH rewards_event_transfer AS (
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
    VALUE:amount / POW(10,6) AS event_rewards_amount,
    VALUE:denom::string AS event_rewards_currency,
    event_attributes:sender::string AS sender,
    event_attributes:recipient::string AS recipient
  FROM {{source('silver_terra', 'msg_events')}} 
    , lateral flatten( input => event_attributes:amount )
  WHERE msg_module = 'distribution'
    AND msg_type = 'distribution/MsgWithdrawDelegationReward'
    AND event_type = 'transfer'
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '1 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),

rewards_event_reward AS (
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
    VALUE:amount / POW(10,6) AS event_rewards_amount,
    VALUE:denom::string AS event_rewards_currency,
    event_attributes:validator::string AS validator
  FROM {{source('silver_terra', 'msg_events')}} 
    , lateral flatten( input => event_attributes:amount )
  WHERE msg_module = 'distribution'
    AND msg_type = 'distribution/MsgWithdrawDelegationReward'
    AND event_type = 'withdraw_rewards'
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '1 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),
  
rewards_event AS (
  SELECT DISTINCT
    rewards_event_transfer.blockchain,
    rewards_event_transfer.chain_id,
    rewards_event_transfer.tx_status,
    rewards_event_transfer.block_id,
    rewards_event_transfer.block_timestamp, 
    rewards_event_transfer.tx_id, 
    rewards_event_transfer.tx_type,
    rewards_event_transfer.msg_module,
    rewards_event_transfer.msg_type, 
    rewards_event_transfer.msg_index,
    rewards_event_transfer.event_attributes,
    rewards_event_transfer.event_rewards_amount,
    rewards_event_transfer.event_rewards_currency,
    rewards_event_reward.validator,
    rewards_event_transfer.sender,
    rewards_event_transfer.recipient
  FROM rewards_event_transfer
  LEFT JOIN rewards_event_reward
  ON rewards_event_transfer.tx_id = rewards_event_reward.tx_id AND rewards_event_transfer.msg_index = rewards_event_reward.msg_index
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
)

SELECT DISTINCT
  rewards_event_base.blockchain,
  rewards_event_base.chain_id,
  rewards_event_base.tx_status,
  rewards_event_base.block_id,
  rewards_event_base.block_timestamp, 
  rewards_event_base.tx_id, 
  rewards_event_base.msg_type, 
  rewards_event.msg_index,
  rewards_event.event_rewards_amount,
  rewards_event.event_rewards_currency,
  sender,
  recipient,
  validator,
  rewards.delegator_address AS delegator
FROM rewards_event_base
LEFT JOIN rewards_event
ON rewards_event_base.tx_id = rewards_event.tx_id
LEFT JOIN rewards
ON rewards_event_base.tx_id = rewards.tx_id