{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'anchor', 'bonds']
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

msgs AS (

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string AS sender,
  msg_value:coins[0]:amount / POW(10,6) AS bonded_amount,
  bonded_amount * price AS bonded_amount_usd,
  msg_value:coins[0]:denom::string as bonded_currency,
  msg_value:execute_msg:bond:validator::string AS validator,
  msg_value:contract::string AS contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}}

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON contract_address = l.address

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND bonded_currency = o.currency 

WHERE msg_value:execute_msg:bond IS NOT NULL
  AND tx_status = 'SUCCEEDED'
  
),

events AS (

SELECT 
  tx_id,
  event_attributes:minted / POW(10,6) AS minted_amount,
  minted_amount * price AS minted_amount_usd, 
  event_attributes:"1_contract_address"::string as minted_currency
FROM {{source('silver_terra', 'msg_events')}}

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND minted_currency = o.currency 

WHERE tx_id IN(SELECT tx_id FROM msgs)
  AND event_type = 'from_contract'
  AND tx_status = 'SUCCEEDED'

)

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  bonded_amount,
  bonded_amount_usd,
  bonded_currency,
  validator,
  minted_amount,
  minted_amount_usd,
  minted_currency,
  contract_address,
  contract_label
FROM msgs m 

JOIN events e 
  ON m.tx_id = e.tx_id