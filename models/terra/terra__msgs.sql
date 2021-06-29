{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra_views', 'msgs']
  )
}}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  tx_module,
  tx_type,
  msg_index,
  msg_module,
  msg_type,
  msg_value
FROM {{source('silver_terra', 'msgs')}}