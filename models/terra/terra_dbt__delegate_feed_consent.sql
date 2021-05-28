{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'oracle']
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
  REGEXP_REPLACE(msg_value:delegate,'\"','') as delegator,
  REGEXP_REPLACE(msg_value:operator,'\"','') as validator,
  msg_value
FROM {{source('terra', 'terra_msgs')}} 
WHERE msg_module = 'oracle' 
  AND msg_type = 'oracle/MsgDelegateFeedConsent'