{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events']
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
FROM {{source('terra', 'terra_msg_events')}}

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}