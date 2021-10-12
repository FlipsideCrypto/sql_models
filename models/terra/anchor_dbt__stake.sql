{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'anchor_dbt', 'stake']
  )
}}

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

msgs AS (

SELECT 
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'unstake' as action,
  msg_value:sender::string as sender,
  msg_value:execute_msg:withdraw_voting_tokens:amount / POW(10,6) as amount,
  msg_value:contract::string as contract_address,
  l.address_name AS contract_label
FROM {{ref('silver_terra__msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

WHERE msg_value:execute_msg:withdraw_voting_tokens IS NOT NULL 
  AND msg_value:contract::string = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
  
),

events AS (

SELECT
  tx_id, 
  price,
  event_attributes:"0_contract_address"::string as currency
FROM {{ref('silver_terra__msg_events')}}

LEFT OUTER JOIN prices r
 ON date_trunc('hour', block_timestamp) = hour
 AND event_attributes:"0_contract_address"::string = r.currency 

WHERE event_type = 'execute_contract'
  AND tx_id IN(SELECT tx_id FROM msgs) 
  AND msg_index = 0
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

)

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  action,
  sender,
  amount,
  amount * price AS amount_usd,
  currency,
  contract_address,
  contract_label
FROM msgs m

JOIN events e 
  ON m.tx_id = e.tx_id

UNION

SELECT 
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'stake' as action,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:amount / POW(10,6) as amount,
  amount * price AS amount_usd,
  msg_value:contract::string as currency,
  msg_value:execute_msg:send:contract::string as contract_address,
  l.address_name AS contract_label
FROM {{ref('silver_terra__msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

LEFT OUTER JOIN prices r
 ON date_trunc('hour', block_timestamp) = hour
 AND msg_value:contract::string = r.currency 

WHERE msg_value:execute_msg:send:msg:stake_voting_tokens IS NOT NULL 
  AND msg_value:execute_msg:send:contract::string = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}