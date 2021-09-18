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

WITH prices AS (

  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price
    FROM {{ ref('terra__oracle_prices')}} 
    GROUP BY 1,2,3

),

stake_msgs AS (

SELECT
  t.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:amount / POW(10,6) as event_amount,
  event_amount * o.price AS event_amount_usd,
  msg_value:contract::string as event_currency,
  msg_value:execute_msg:send:contract::string as contract_address,
  l.address_name AS contract_label 
FROM {{source('silver_terra', 'msgs')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND msg_value:contract::string = o.currency 

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

WHERE msg_value:execute_msg:send:msg:stake_voting_tokens IS NOT NULL 
  AND msg_value:execute_msg:send:contract::string = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
  AND tx_status = 'SUCCEEDED'

),

stake_events AS (
SELECT 
  tx_id,
  event_attributes:share as shares
FROM {{source('silver_terra', 'msg_events')}}
WHERE tx_id IN(SELECT DISTINCT tx_id FROM stake_msgs)
  AND event_type = 'from_contract'
)

-- Staking 
SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  'stake' as event_type,
  sender,
  event_amount,
  event_amount_usd,  
  event_currency, 
  shares,
  contract_address,
  contract_label 
FROM stake_msgs m 

JOIN stake_events e 
  ON m.tx_id = e.tx_id

UNION 

-- Unstaking

SELECT
  t.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'unstake' as event_type,
  msg_value:sender::string as sender,
  msg_value:execute_msg:withdraw_voting_tokens:amount / POW(10,6) as event_amount,
  event_amount * o.price AS event_amount_usd,
  'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' as event_currency,
  NULL as shares,
  msg_value:contract::string as contract_address,
  l.address_name as contract_label
FROM {{source('silver_terra', 'msgs')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND 'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' = o.currency 

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

WHERE msg_value:execute_msg:withdraw_voting_tokens IS NOT NULL
  AND msg_value:contract::string = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
  AND tx_status = 'SUCCEEDED'