{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
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
  tx_type,
  msg_module,
  msg_type, 
  event_type,
  event_attributes,
  event_attributes:grant_type::string AS grant_type,
  event_attributes:grantee::string AS grantee,
  event_attributes:granter::string AS granter
FROM {{source('silver_terra', 'msg_events')}} 
WHERE msg_module = 'msgauth'
AND event_type = 'grant_authorization'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
-- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}