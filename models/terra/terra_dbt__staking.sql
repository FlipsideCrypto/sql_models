{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
  )
}}


WITH staking_msg_events AS (
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
    event_attributes:amount AS amount,
    event_attributes:validator::string AS validator
  FROM {{source('terra', 'terra_msg_events')}}
  WHERE msg_module = 'staking'
  AND event_type = 'delegate'
  {% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),
staking_msg AS (
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
)

SELECT
    staking_msg.blockchain,
    staking_msg.chain_id,
    staking_msg.tx_status,
    staking_msg.block_id,
    staking_msg.block_timestamp, 
    staking_msg.tx_id, 
    staking_msg.msg_type, 
    delegator_address,
    CASE WHEN validator_address IS NULL THEN validator ELSE validator_address END AS validator_address,
    CASE WHEN event_amount IS NULL THEN amount ELSE event_amount END AS event_amount,
    event_currency
FROM staking_msg
LEFT JOIN 
staking_msg_events
ON staking_msg.tx_id = staking_msg_events.tx_id