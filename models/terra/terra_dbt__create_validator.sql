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
    event_attributes:action::string AS action,
    event_attributes:sender::string AS sender,
    event_attributes:module::string AS module,
    event_attributes:amount AS create_validator_amount,
    event_attributes:validator::string AS validator
FROM {{source('silver_terra', 'msg_events')}}
WHERE msg_module = 'staking' 
AND msg_type = 'staking/MsgCreateValidator'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
-- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
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
  REGEXP_REPLACE(msg_value:pubkey,'\"','') as pubkey,
  REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address,
  REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
  REGEXP_REPLACE(msg_value:description:details,'\"','') as details,
  REGEXP_REPLACE(msg_value:description:identity,'\"','') as identity,
  REGEXP_REPLACE(msg_value:description:moniker,'\"','') as moniker,
  REGEXP_REPLACE(msg_value:description:website,'\"','') as website,
  REGEXP_REPLACE(msg_value:commission:max_change_rate,'\"','') as max_change_rate,
  REGEXP_REPLACE(msg_value:commission:max_rate,'\"','') as max_rate,
  REGEXP_REPLACE(msg_value:commission:rate,'\"','') as rate,
  REGEXP_REPLACE(msg_value:min_self_delegation,'\"','') as min_self_delegation,
  msg_value:value:amount as amount,
  REGEXP_REPLACE(msg_value:value:denom,'\"','') as denom
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgCreateValidator'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
-- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
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
