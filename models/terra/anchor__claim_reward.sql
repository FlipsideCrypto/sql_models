{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'anchor', 'reward']
  )
}}

WITH prices AS (

  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price
    FROM {{ ref('terra__oracle_prices')}} 
    GROUP BY 1,2,3

),

withdraw_msgs AS (

SELECT 
  tx_id,
  msg_index,
  msg_value:contract::string as claim_0_contract
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_value:execute_msg:withdraw IS NOT NULL
  AND msg_index = 0
  AND msg_value:contract::string = 'terra1897an2xux840p9lrh6py3ryankc6mspw49xse3'
  AND tx_status = 'SUCCEEDED'

),

claim_msgs AS (

SELECT 
  tx_id,
  msg_index,
  msg_value:sender::string as sender,
  msg_value:contract::string as claim_1_contract
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_value:execute_msg:claim_rewards IS NOT NULL
  AND msg_index = 1
  AND msg_value:contract::string = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
  AND tx_status = 'SUCCEEDED'
  
),

withdraw AS (  

SELECT 
  m.tx_id,
  claim_0_contract,
  event_attributes:"0_amount" / POW(10,6) as claim_0_amount,
  claim_0_amount * price AS claim_0_amount_usd,
  event_attributes:"1_contract_address"::string as claim_0_currency
FROM {{source('silver_terra', 'msg_events')}} e
  
JOIN withdraw_msgs m 
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND claim_0_currency = o.currency 

WHERE event_type = 'from_contract'  
  AND tx_status = 'SUCCEEDED'
  
),

claim AS (

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  claim_1_contract,
  event_attributes:claim_amount / POW(10,6) as claim_1_amount,
  claim_1_amount * price AS claim_1_amount_usd,
  event_attributes:"2_contract_address"::string as claim_1_currency
FROM {{source('silver_terra', 'msg_events')}} e
  
JOIN claim_msgs m 
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND claim_1_currency = o.currency 
  
WHERE event_type = 'from_contract'
  AND tx_status = 'SUCCEEDED'
  
)

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  c.tx_id,
  sender,
  claim_0_amount,
  claim_0_amount_usd,
  claim_0_currency,
  claim_0_contract,
  l0.address_name AS claim_0_contract_label,
  claim_1_amount,
  claim_1_amount_usd,
  claim_1_currency,
  claim_1_contract,
  l1.address_name AS claim_1_contract_label
FROM claim c 

JOIN withdraw w 
  ON c.tx_id = w.tx_id

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l0
ON claim_0_contract = l0.address

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l1
ON claim_1_contract = l1.address