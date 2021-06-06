{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
  )
}}


WITH staking_events AS (
    SELECT * FROM {{ ref('terra_dbt__create_validator_events') }}
),

staking AS (
    SELECT * FROM {{ ref('terra_dbt__create_validator') }}
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
    sender
  FROM staking_events 
  WHERE event_type = 'message' AND msg_index = 0
), 

create_validator AS (
  SELECT
    tx_id,
    create_validator_amount,
    validator
  FROM staking_events 
  WHERE event_type = 'create_validator' 
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
  sender,
  create_validator_amount,
  validator,
  staking.pubkey,
  staking.delegator_address,
  staking.details,
  staking.identity,
  staking.moniker,
  staking.website,
  staking.max_change_rate,
  staking.max_rate,
  staking.rate,
  staking.min_self_delegation,
  staking.amount,
  staking.denom
FROM event_base
LEFT JOIN message
ON event_base.tx_id = message.tx_id
LEFT JOIN create_validator
ON event_base.tx_id = create_validator.tx_id
LEFT JOIN staking
ON event_base.tx_id = staking.tx_id
