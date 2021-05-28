{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'unjail']
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
  REGEXP_REPLACE(msg_value:address,'\"','') as address
FROM {{source('terra', 'terra_msgs')}}  
WHERE msg_module = 'cosmos' 