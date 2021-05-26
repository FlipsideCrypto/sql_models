{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events_msgauth_grant]
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
  event_attributes:grant_type::string AS grant_type,
  event_attributes:grantee::string AS grantee,
  event_attributes:granter::string AS granter
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'msgauth'
AND event_type = 'grant_authorization'