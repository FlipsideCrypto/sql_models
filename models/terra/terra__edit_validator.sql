{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
  )
}}


WITH staking_events AS (
    SELECT * FROM {{ ref('terra_dbt__edit_validator_events') }}
),

staking AS (
    SELECT * FROM {{ ref('terra_dbt__edit_validator') }}
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
  WHERE event_type = 'message'
), 

edit_validator AS (
  SELECT
    tx_id,
    commission_rate,
    commission_maxRate,
    commission_maxChangeRate,
    commission_updateTime
  FROM staking_events 
  WHERE event_type = 'edit_validator' 
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
  edit_validator.commission_rate,
  commission_maxRate,
  commission_maxChangeRate,
  commission_updateTime,
  staking.validator,
  staking.commission_rate,
  staking.details,
  staking.identity,
  staking.moniker,
  staking.security_contact,
  staking.website
FROM event_base
LEFT JOIN message
ON event_base.tx_id = message.tx_id
LEFT JOIN edit_validator
ON event_base.tx_id = edit_validator.tx_id
LEFT JOIN staking
ON event_base.tx_id = staking.tx_id