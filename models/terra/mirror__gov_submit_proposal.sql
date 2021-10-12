{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'mirror', 'mirror_gov']
) }}

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
FROM {{ref('silver_terra__msgs')}}
WHERE 
--msg_value:execute_msg:send:msg:create_poll IS NOT NULL 
   msg_value:execute_msg:send:contract::string = 'terra1wh39swv7nq36pnefnupttm2nr96kz7jjddyt2x' -- MIR Governance 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
),

events AS (
SELECT 
  tx_id,
  COALESCE(to_timestamp(event_attributes:end_time),event_attributes:end_height) as end_time,
  event_attributes:poll_id::number as poll_id
FROM {{ref('silver_terra__msg_events')}}
WHERE tx_id IN(select tx_id from msgs)
  and event_attributes:"0_action"::string = 'send'
  AND event_type = 'from_contract'
  and poll_id is not null 

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
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
