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
  REGEXP_REPLACE(msg_value:address,'\"','') as validator,
  REGEXP_REPLACE(msg_value:commission_rate,'\"','') as commission_rate,
  REGEXP_REPLACE(msg_value:Description:details,'\"','') as details,
  REGEXP_REPLACE(msg_value:Description:identity,'\"','') as identity,
  REGEXP_REPLACE(msg_value:Description:moniker,'\"','') as moniker,
  REGEXP_REPLACE(msg_value:Description:security_contact,'\"','') as security_contact,
  REGEXP_REPLACE(msg_value:Description:website,'\"','') as website
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgEditValidator'
  AND chain_id = 'columbus-3'

UNION 

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  REGEXP_REPLACE(msg_value:address,'\"','') as validator,
  REGEXP_REPLACE(msg_value:commission_rate,'\"','') as commission_rate,
  REGEXP_REPLACE(msg_value:description:details,'\"','') as details,
  REGEXP_REPLACE(msg_value:description:identity,'\"','') as identity,
  REGEXP_REPLACE(msg_value:description:moniker,'\"','') as moniker,
  REGEXP_REPLACE(msg_value:description:security_contact,'\"','') as security_contact,
  REGEXP_REPLACE(msg_value:description:website,'\"','') as website,
  msg_value
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgEditValidator'
  AND chain_id = 'columbus-4'