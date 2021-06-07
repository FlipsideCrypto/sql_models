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
  REGEXP_REPLACE(msg_value:denom,'\"','') as currency,
  msg_value:exchange_rate as rate,
  REGEXP_REPLACE(msg_value:feeder,'\"','') as feeder,
  REGEXP_REPLACE(msg_value:salt,'\"','') as salt,
  REGEXP_REPLACE(msg_value:validator,'\"','') as validator
FROM {{source('terra', 'terra_msgs')}}  
WHERE msg_module = 'oracle' 
  AND msg_type = 'oracle/MsgExchangeRateVote'