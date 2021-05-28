{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'grant']
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
  REGEXP_REPLACE(msg_value:grantee,'\"','') as grantee,
  REGEXP_REPLACE(msg_value:msgs[0]:type,'\"','') as type,
  REGEXP_REPLACE(msg_value:msgs[0]:value:amount[0]:amount,'\"','') as amount,
  REGEXP_REPLACE(msg_value:msgs[0]:value:amount[0]:denom,'\"','') as currency,
  REGEXP_REPLACE(msg_value:msgs[0]:value:from_address,'\"','') as event_from,
  REGEXP_REPLACE(msg_value:msgs[0]:value:to_address,'\"','') as event_to
  
FROM {{source('terra', 'terra_msgs')}}  
WHERE msg_module = 'msgauth'
  AND msg_type = 'msgauth/MsgExecAuthorized'