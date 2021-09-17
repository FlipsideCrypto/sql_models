{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'anchor', 'deposits']
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
  msg_value:sender::string as sender,
  msg_value:coins[0]:amount / POW(10,6) as deposit_amount,
  deposit_amount * price AS deposit_amount_usd,
  msg_value:coins[0]:denom::string as deposit_currency,
  msg_value:contract::string as contract_address,
  l.address_name as contract_label
FROM {{source('silver_terra', 'msgs')}}

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND deposit_currency = o.currency 

WHERE msg_value:execute_msg:deposit_stable IS NOT NULL 
  AND msg_value:contract::string = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
  AND tx_status = 'SUCCEEDED'

),

events AS (

SELECT 
  tx_id,
  event_attributes:mint_amount / POW(10,6) as mint_amount,
  mint_amount * price AS mint_amount_usd,
  event_attributes:"1_contract_address"::string as mint_currency
FROM {{source('silver_terra', 'msg_events')}}

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND mint_currency = o.currency 

WHERE event_type = 'from_contract'
  AND tx_id IN(SELECT tx_id FROM msgs)
  AND tx_status = 'SUCCEEDED'

)

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  deposit_amount,
  deposit_amount_usd,
  deposit_currency,
  mint_amount,
  mint_amount_usd,
  mint_currency,
  contract_address,
  contract_label
FROM msgs m 

JOIN events e 
  ON m.tx_id = e.tx_id