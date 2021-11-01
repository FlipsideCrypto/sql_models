{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'anchor', 'reward_claims']
) }}
WITH prices AS (

  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price
    FROM {{ ref('terra__oracle_prices')}} 
    
    WHERE 1=1
    
    {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
    {% endif %}

    GROUP BY 1,2,3

),

withdraw_msgs AS (

SELECT 
  tx_id,
  msg_index,
  msg_value:contract::string as claim_0_contract
FROM {{ref('silver_terra__msgs')}}
WHERE msg_value:execute_msg:withdraw IS NOT NULL
  AND msg_index = 0
  AND msg_value:contract::string = 'terra1897an2xux840p9lrh6py3ryankc6mspw49xse3'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

),

claim_msgs AS (

SELECT 
  tx_id,
  msg_index,
  msg_value:sender::string as sender,
  msg_value:contract::string as claim_1_contract
FROM {{ref('silver_terra__msgs')}}
WHERE msg_value:execute_msg:claim_rewards IS NOT NULL
  AND msg_index = 1
  AND msg_value:contract::string = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
  
),

withdraw AS (  

SELECT 
  m.tx_id,
  claim_0_contract,
  event_attributes:"0_amount" / POW(10,6) as claim_0_amount,
  event_attributes:"1_contract_address"::string as claim_0_currency
FROM {{ref('silver_terra__msg_events')}} e
  
JOIN withdraw_msgs m 
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index

WHERE event_type = 'from_contract'  
  AND tx_status = 'SUCCEEDED'
  AND event_attributes:"0_action"::string = 'withdraw'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
  
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
  event_attributes:"2_contract_address"::string as claim_1_currency
FROM {{ref('silver_terra__msg_events')}} e
  
JOIN claim_msgs m 
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index
  
WHERE event_type = 'from_contract'
  AND tx_status = 'SUCCEEDED'
  AND event_attributes:"0_action"::string = 'claim_rewards'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
  
)

SELECT 
  c.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  c.tx_id,
  sender,
  claim_0_amount,
  claim_0_amount * p0.price AS claim_0_amount_usd,
  claim_0_currency,
  claim_0_contract,
  l0.address AS claim_0_contract_label,
  claim_1_amount,
  claim_1_amount * p1.price AS claim_1_amount_usd,
  claim_1_currency,
  claim_1_contract,
  l1.address AS claim_1_contract_label
FROM claim c 

JOIN withdraw w 
  ON c.tx_id = w.tx_id

LEFT OUTER JOIN {{ref('silver_crosschain__address_labels')}} as l0
ON claim_0_contract = l0.address

LEFT OUTER JOIN {{ref('silver_crosschain__address_labels')}} as l1
ON claim_1_contract = l1.address

LEFT OUTER JOIN prices p0
 ON date_trunc('hour', block_timestamp) = p0.hour
 AND claim_0_currency = p0.currency 

 LEFT OUTER JOIN prices p1
 ON date_trunc('hour', block_timestamp) = p1.hour
 AND claim_1_currency = p1.currency 