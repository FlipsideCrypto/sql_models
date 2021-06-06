{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
  )
}}


WITH staking_events AS (
    SELECT * FROM {{ ref('terra_dbt__undelegate_events') }}
),
staking AS (
    SELECT * FROM {{ ref('terra_dbt__undelegate') }}
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
    1_sender,
    2_sender
  FROM staking_events 
  WHERE event_type = 'message' AND msg_index = 0
), 

transfer AS (
  SELECT
    tx_id, 
    event_transfer_0_amount,
    event_transfer_0_currency,
    event_transfer_0_recipient,
    event_transfer_0_sender,
    event_transfer_1_amount,
    event_transfer_1_currency,
    event_transfer_1_recipient,
    event_transfer_1_sender
  FROM staking_events 
  WHERE event_type = 'transfer' AND msg_index = 0
),

unbond AS (
  SELECT
    tx_id,
    unbond_amount,
    validator,
    completion_time
  FROM staking_events 
  WHERE event_type = 'unbond' 
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
  2_sender,
  event_transfer_0_amount,
  event_transfer_0_currency,
  event_transfer_0_recipient,
  event_transfer_0_sender,
  event_transfer_1_amount,
  event_transfer_1_currency,
  event_transfer_1_recipient,
  event_transfer_1_sender,
  unbond_amount,
  validator,
  completion_time,
  staking.delegator_address,
  staking.validator_src_address,
  staking.validator_dst_address
FROM event_base
LEFT JOIN message
ON event_base.tx_id = message.tx_id
LEFT JOIN transfer
ON event_base.tx_id = transfer.tx_id
LEFT JOIN unbond
ON event_base.tx_id = unbond.tx_id
LEFT JOIN staking
ON event_base.tx_id = staking.tx_id