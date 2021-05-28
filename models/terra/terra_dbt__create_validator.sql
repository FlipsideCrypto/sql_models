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
  msg_type, 
  REGEXP_REPLACE(msg_value:pubkey,'\"','') as pubkey,
  REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address,
  REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
  REGEXP_REPLACE(msg_value:description:details,'\"','') as details,
  REGEXP_REPLACE(msg_value:description:identity,'\"','') as identity,
  REGEXP_REPLACE(msg_value:description:moniker,'\"','') as moniker,
  REGEXP_REPLACE(msg_value:description:website,'\"','') as website,
  REGEXP_REPLACE(msg_value:commission:max_change_rate,'\"','') as max_change_rate,
  REGEXP_REPLACE(msg_value:commission:max_rate,'\"','') as max_rate,
  REGEXP_REPLACE(msg_value:commission:rate,'\"','') as rate,
  REGEXP_REPLACE(msg_value:min_self_delegation,'\"','') as min_self_delegation,
  msg_value:value:amount as amount,
  REGEXP_REPLACE(msg_value:value:denom,'\"','') as denom
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgCreateValidator'