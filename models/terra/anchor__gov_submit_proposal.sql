{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'anchor', 'anchor_gov']
  )
}}

WITH msgs AS (
SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as creator,
  msg_value:execute_msg:send:amount / POW(10,6) as amount,
  msg_value:execute_msg:send:msg:create_poll:title::string as title,
  msg_value:execute_msg:send:msg:create_poll:link::string as link,
  msg_value:execute_msg:send:msg:create_poll:description::string as description,
  msg_value:execute_msg:send:msg:create_poll:execute_msg:msg as msg,
  msg_value:execute_msg:send:contract::string as contract_address
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_value:execute_msg:send:msg:create_poll IS NOT NULL 
  AND msg_value:execute_msg:send:contract::string = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5' -- ANC Governance 
  AND tx_status = 'SUCCEEDED'
),

events AS (
SELECT 
  tx_id,
  to_timestamp(event_attributes:end_time) as end_time,
  event_attributes:poll_id as poll_id
FROM {{source('silver_terra', 'msg_events')}}
WHERE tx_id IN(select tx_id from msgs)
  AND event_type = 'from_contract'
)

SELECT 
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  poll_id,
  end_time,
  creator,
  amount,
  title,
  link,
  description,
  msg,
  contract_address,
  l.address_name AS contract_label
FROM msgs m 

JOIN events e 
  ON m.tx_id = e.tx_id

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON contract_address = l.address
