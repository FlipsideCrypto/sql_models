{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'reward']
  )
}}
WITH commissions_event AS (
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
    event_attributes:amount[0]:amount / POW(10,6) AS amount,
    event_attributes:amount:denom::string AS currency,
    event_attributes:recipient::string AS recipient,
    event_attributes:"0_sender"::string AS "0_sender",
    event_attributes:"1_sender"::string AS "1_sender",
    event_attributes:action::string AS action,
    event_attributes:module::string AS module
  FROM {{source('terra', 'terra_msg_events')}}
  WHERE msg_module = 'distribution' 
  AND msg_type = 'distribution/MsgWithdrawValidatorCommission' 
  {% if is_incremental() %}
  AND block_timestamp >= getdate() - interval '1 days'
  {% else %}
  AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
),

commissions AS (
  SELECT 
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tx_id, 
    msg_type, 
    REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address
  FROM {{source('terra', 'terra_msgs')}} 
  WHERE msg_module = 'distribution' 
    AND msg_type = 'distribution/MsgWithdrawValidatorCommission' 
    {% if is_incremental() %}
  AND block_timestamp >= getdate() - interval '1 days'
  {% else %}
  AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
),

commissions_event_base AS (
  SELECT DISTINCT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tx_id, 
    msg_type
  FROM commissions 
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
  commissions_event_base.msg_type, 
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