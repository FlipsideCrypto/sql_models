{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'terraswap', 'lp']
) }}

-- LP Un-staking
WITH msgs AS (

SELECT 
  blockchain,
  chain_id,  
  block_id,
  block_timestamp,
  tx_id,
  'unstake_lp' as event_type,
  msg_value:sender::string as sender,
  msg_value:execute_msg:unbond:amount / POW(10,6) as amount
FROM {{ref('silver_terra__msgs')}}
WHERE msg_value:execute_msg:unbond IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

),

events AS (

SELECT 
  tx_id,
  event_attributes:"0_contract_address"::string as contract_address
FROM {{ref('silver_terra__msg_events')}}
where tx_id IN(SELECT distinct tx_id from msgs)
  AND event_type = 'execute_contract'
  AND msg_index = 0

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

)

-- unstake
SELECT 
  m.blockchain,
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
  m.blockchain,
  chain_id,  
  block_id,
  block_timestamp,
  tx_id,
  'stake_lp' as event_type,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:amount / POW(10,6) as amount,
  msg_value:contract::string as contract_address,
  address_name as contract_label
FROM {{ref('silver_terra__msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} 
  ON msg_value:contract::string = address

WHERE msg_value:execute_msg:send:msg:bond IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}