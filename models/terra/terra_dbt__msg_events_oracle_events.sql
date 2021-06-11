{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events_oracle']
  )
}}

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  tx_type,
  msg_module,
  msg_type, 
  event_type,
  event_attributes,
  event_attributes:denom::string AS currency,
  event_attributes:feeder::string AS feeder,
  event_attributes:voter::string AS voter
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'oracle'
AND event_type = 'vote'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}