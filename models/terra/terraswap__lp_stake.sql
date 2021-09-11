{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'terraswap', 'lp']
  )
}}

-- LP Un-staking
WITH msgs AS (

SELECT 
  chain_id,  
  block_id,
  block_timestamp,
  tx_id,
  'unstake_lp' as event_type,
  msg_value:sender::string as sender,
  msg_value:execute_msg:unbond:amount / POW(10,6) as amount
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_value:execute_msg:unbond IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

),

events AS (

SELECT 
  tx_id,
  event_attributes:"0_contract_address"::string as contract_address
FROM {{source('silver_terra', 'msg_events')}}
where tx_id IN(SELECT distinct tx_id from msgs)
  AND event_type = 'execute_contract'
  AND msg_index = 0

)

-- unstake
SELECT 
  chain_id,  
  block_id,
  block_timestamp,
  m.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  address_name as contract_label
FROM msgs m

JOIN events e 
  ON m.tx_id = e.tx_id

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}}
  ON contract_address = address

UNION

-- stake 
SELECT 
  chain_id,  
  block_id,
  block_timestamp,
  tx_id,
  'stake_lp' as event_type,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:amount / POW(10,6) as amount,
  msg_value:contract::string as contract_address,
  address_name as contract_label
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} l
  ON m.msg_value:contract::string = l.address

WHERE msg_value:execute_msg:send:msg:bond IS NOT NULL 
  AND tx_status = 'SUCCEEDED'