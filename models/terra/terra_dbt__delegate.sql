{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'staking']
  )
}}

WITH staking_events AS (
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
    msg_index,
    event_type,
    event_attributes,
    event_attributes:validator::string AS validator,
    -- CASE WHEN event_type = 'delegate' THEN event_attributes:amount / POW(10,6) ELSE NULL END AS delegated_amount,
    event_attributes:"0_sender"::string AS "0_sender",
    event_attributes:"1_sender"::string AS "1_sender",
    event_attributes:action::string AS action,
    event_attributes:module::string AS module,
    CASE WHEN event_type = 'transfer' THEN event_attributes:amount:amount / POW(10,6) ELSE NULL END AS event_transfer_amount,
    CASE WHEN event_type = 'transfer' THEN event_attributes:amount:denom::string ELSE NULL END AS event_transfer_currency,
    event_attributes:sender::string AS sender,
    event_attributes:recipient::string AS recipient
  FROM {{source('terra', 'terra_msg_events')}}
  WHERE msg_module = 'staking' 
    AND msg_type = 'staking/MsgDelegate'

  {% if is_incremental() %}
  AND block_timestamp >= getdate() - interval '1 days'
  {% else %}
  AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
),

staking AS (
  SELECT 
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tx_id, 
    msg_type, 
    REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
    REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address,
    REGEXP_REPLACE(msg_value:amount:amount,'\"','') as event_amount,
    REGEXP_REPLACE(msg_value:amount:denom,'\"','') as event_currency
  FROM {{source('terra', 'terra_msgs')}} 
  WHERE msg_module = 'staking' 
    AND msg_type = 'staking/MsgDelegate'
  {% if is_incremental() %}
  AND block_timestamp >= getdate() - interval '1 days'
  {% else %}
  AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
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
    "0_sender",
    "1_sender"
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
    validator
    -- delegated_amount
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
  "0_sender",
  "1_sender",
  CASE WHEN event_transfer_amount IS NOT NULL THEN event_transfer_amount / POW(10,6) ELSE staking.event_amount / POW(10,6) END AS event_transfer_amount,
  CASE WHEN event_transfer_currency IS NOT NULL THEN event_transfer_currency ELSE staking.event_currency END AS event_transfer_currency,
  sender,
  recipient,
  -- delegated_amount / POW(10,6),
  staking.delegator_address,
  staking.validator_address AS validator
FROM event_base
LEFT JOIN message
ON event_base.tx_id = message.tx_id
LEFT JOIN transfer
ON event_base.tx_id = transfer.tx_id
LEFT JOIN delegate
ON event_base.tx_id = delegate.tx_id
LEFT JOIN staking
ON event_base.tx_id = staking.tx_id