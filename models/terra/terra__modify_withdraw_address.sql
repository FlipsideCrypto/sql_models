{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'reward']
  )
}}

WITH rewards_event AS (
  SELECT * FROM {{ ref('terra_dbt__modify_withdraw_address_events') }}
),

rewards AS (
  SELECT * FROM {{ ref('terra_dbt__modify_withdraw_address') }}
),

rewards_event_base AS (
  SELECT DISTINCT
      blockchain,
      chain_id,
      tx_status,
      block_id,
      block_timestamp, 
      tx_id, 
      tx_type,
      msg_module,
      msg_type, 
      msg_index
  FROM rewards_event 
), 

message AS (
  SELECT
      tx_id, 
      action,
      module,
      sender
  FROM rewards_event 
  WHERE event_type = 'message' AND msg_index = 0
)


SELECT
  rewards_event_base.blockchain,
  rewards_event_base.chain_id,
  rewards_event_base.tx_status,
  rewards_event_base.block_id,
  rewards_event_base.block_timestamp, 
  rewards_event_base.tx_id, 
  rewards_event_base.tx_type,
  rewards_event_base.msg_module,
  rewards_event_base.msg_type, 
  rewards_event_base.msg_index,
  action,
  module,
  sender,
  rewards.withdraw_address
FROM rewards_event_base
LEFT JOIN message
ON rewards_event_base.tx_id = message.tx_id
LEFT JOIN rewards
ON rewards_event_base.tx_id = rewards.tx_id