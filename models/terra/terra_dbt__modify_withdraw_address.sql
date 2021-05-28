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
  REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
  REGEXP_REPLACE(msg_value:withdraw_address,'\"','') as withdraw_address
FROM {{source('terra', 'terra_msgs')}} 
WHERE msg_module = 'distribution' 
  AND msg_type = 'distribution/MsgModifyWithdrawAddress' 