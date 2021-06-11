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
  msg_type, 
  msg_index,
  REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
  REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address,
  REGEXP_REPLACE(msg_value:amount:amount,'\"','') as event_amount,
  REGEXP_REPLACE(msg_value:amount:denom,'\"','') as event_currency
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'distribution' 
AND msg_type = 'distribution/MsgWithdrawDelegationReward'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}