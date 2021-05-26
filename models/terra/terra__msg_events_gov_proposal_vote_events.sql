{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events_gov_proposal_vote']
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
  event_attributes:option::string AS option,
  event_attributes:proposal_id AS proposal_id,
  event_attributes:validator::string AS validator
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'gov'
AND event_type = 'proposal_vote'