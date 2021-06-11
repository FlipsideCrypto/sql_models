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
  event_attributes:amount:amount AS event_rewards_amount,
  event_attributes:amount:denom::string AS event_rewards_currency,
  event_attributes:validator::string AS validator,
  event_attributes:amount:amount AS event_transfer_amount,
  event_attributes:amount:denom::string AS event_transfer_currency,
  event_attributes:sender::string AS sender,
  event_attributes:recipient::string AS recipient,
  event_attributes:action::string AS action
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'distribution'
AND msg_type = 'distribution/MsgWithdrawDelegationReward'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
