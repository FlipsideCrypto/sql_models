{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'gov']
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
  REGEXP_REPLACE(msg_value:voter,'\"','') as voter,
  REGEXP_REPLACE(msg_value:proposal_id,'\"','') as proposal_id,
  REGEXP_REPLACE(msg_value:option,'\"','') as "option"
FROM {{source('terra', 'terra_msgs')}} 
WHERE msg_module = 'gov' 
  AND msg_type = 'gov/MsgVote'