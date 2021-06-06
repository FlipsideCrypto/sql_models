{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
  )
}}


WITH staking_events AS (
    SELECT * FROM {{ ref('terra_dbt__delegate_events') }}
),

staking AS (
    SELECT * FROM {{ ref('terra_dbt__delegate') }}
), 

event_base AS (
  SELECT DISTINCT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tx_id, 
    msg_type
  FROM staking 
), 

message AS (
  SELECT
    tx_id, 
    action,
    module,
    0_sender,
    1_sender
  FROM staking_events 
  WHERE event_type = 'message' AND msg_index = 0
), 

transfer AS (
  SELECT
      tx_id, 
      event_transfer_amount,
      event_transfer_currency,
      sender,
      recipient
  FROM staking_events 
  WHERE event_type = 'transfer' AND msg_index = 0
),

delegate AS (
  SELECT
    tx_id,
    validator,
    delegated_amount
  FROM staking_events 
  WHERE event_type = 'delegate' 
)

SELECT
  event_base.blockchain,
  event_base.chain_id,
  event_base.tx_status,
  event_base.block_id,
  event_base.block_timestamp, 
  event_base.tx_id, 
  event_base.msg_type, 
  action,
  module,
  0_sender,
  1_sender,
  event_transfer_amount,
  event_transfer_currency,
  sender,
  recipient,
  validator,
  delegated_amount,
  staking.delegator_address
FROM event_base
LEFT JOIN message
ON event_base.tx_id = message.tx_id
LEFT JOIN transfer
ON event_base.tx_id = transfer.tx_id
LEFT JOIN delegate
ON event_base.tx_id = delegate.tx_id
LEFT JOIN staking
ON event_base.tx_id = staking.tx_id
  