{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra_views', 'msg_events']
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
  event_index,
  event_type,
  event_attributes
FROM {{source('silver_terra', 'msg_events')}}