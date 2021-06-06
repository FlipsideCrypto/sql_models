{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'reward']
  )
}}

WITH commissions_event AS (
  SELECT * FROM {{ ref('terra_dbt__modify_withdraw_address_events') }}
),

commissions AS (
  SELECT * FROM {{ ref('terra_dbt__modify_withdraw_address') }}
),

commissions_event_base AS (
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
  FROM commissions_event 
), 

message AS (
  SELECT
    tx_id, 
    action,
    module,
    "0_sender",
    "1_sender"
  FROM commissions_event 
  WHERE event_type = 'message' 
),

transfer AS (
  SELECT
    tx_id, 
    amount,
    currency,
    recipient
  FROM commissions_event 
  WHERE event_type = 'transfer' 
)

SELECT
  commissions_event_base.blockchain,
  commissions_event_base.chain_id,
  commissions_event_base.tx_status,
  commissions_event_base.block_id,
  commissions_event_base.block_timestamp, 
  commissions_event_base.tx_id, 
  commissions_event_base.tx_type,
  commissions_event_base.msg_module,
  commissions_event_base.msg_type, 
  commissions_event_base.msg_index,
  action,
  module,
  "0_sender",
  "1_sender",
  amount,
  currency,
  recipient,
  commissions.validator_address
FROM commissions_event_base
LEFT JOIN message
ON commissions_event_base.tx_id = message.tx_id
LEFT JOIN transfer
ON commissions_event_base.tx_id = transfer.tx_id
LEFT JOIN commissions
ON commissions_event_base.tx_id = commissions.tx_id