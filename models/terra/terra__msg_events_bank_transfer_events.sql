{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'msg_events_bank_transfer']
  )
}}

WITH input AS (
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
    key,
    SPLIT(key, '_')[0]::string AS message_id,
    SPLIT(key, '_')[1]::string AS message_attribute,
    value
  FROM {{source('terra', 'terra_msg_events')}}
  , lateral flatten(input => event_attributes) vm
  WHERE msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
    AND event_type = 'transfer'
),
tbl_recipient AS (
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
    value AS recipient,
    message_id    
  FROM input
  WHERE message_attribute = 'recipient'
),
tbl_amount AS (
  SELECT
      tx_id, 
      message_id,
      value:amount AS amount,
      value:denom::string AS currency
  FROM input
  WHERE message_attribute = 'amount'
)
SELECT 
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp, 
    tbl_recipient.tx_id, 
    tx_type,
    msg_module,
    msg_type, 
    event_type,
    event_attributes,
    recipient,
    tbl_amount.amount,
    tbl_amount.currency,
    tbl_recipient.message_id
FROM tbl_recipient
LEFT JOIN tbl_amount
ON tbl_recipient.tx_id = tbl_amount.tx_id AND tbl_recipient.message_id = tbl_amount.message_id
