{{ 
  config(
    materialized='view', 
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
  event_attributes:commission_rate::string AS commission_rate_attribute,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[0], ': ')[1]::float AS commission_rate,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[1], ': ')[1]::float AS commission_maxRate,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[2], ': ')[1]::float AS commission_maxChangeRate,
  SPLIT(SPLIT(event_attributes:commission_rate::string, ',')[3], ': ')[1]::string AS commission_updateTime,
  event_attributes:min_self_delegation AS min_self_delegation
FROM {{source('terra', 'terra_msg_events')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgEditValidator'
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
  REGEXP_REPLACE(msg_value:address,'\"','') as validator,
  REGEXP_REPLACE(msg_value:commission_rate,'\"','') as commission_rate,
  REGEXP_REPLACE(msg_value:Description:details,'\"','') as details,
  REGEXP_REPLACE(msg_value:Description:identity,'\"','') as identity,
  REGEXP_REPLACE(msg_value:Description:moniker,'\"','') as moniker,
  REGEXP_REPLACE(msg_value:Description:security_contact,'\"','') as security_contact,
  REGEXP_REPLACE(msg_value:Description:website,'\"','') as website
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgEditValidator'
  AND chain_id = 'columbus-3'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION 

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  REGEXP_REPLACE(msg_value:address,'\"','') as validator,
  REGEXP_REPLACE(msg_value:commission_rate,'\"','') as commission_rate,
  REGEXP_REPLACE(msg_value:description:details,'\"','') as details,
  REGEXP_REPLACE(msg_value:description:identity,'\"','') as identity,
  REGEXP_REPLACE(msg_value:description:moniker,'\"','') as moniker,
  REGEXP_REPLACE(msg_value:description:security_contact,'\"','') as security_contact,
  REGEXP_REPLACE(msg_value:description:website,'\"','') as website,
  msg_value
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgEditValidator'
  AND chain_id = 'columbus-4'
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