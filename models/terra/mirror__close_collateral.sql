{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'mirror', 'collateral']
  )
}}

WITH prices AS (

  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price_usd
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
  msg_value:execute_msg:send:msg:burn:position_idx as collateral_id,
  msg_value:sender::string as sender,
  msg_value:contract::string as contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}}

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

WHERE msg_value:execute_msg:send:msg:burn IS NOT NULL 
  AND tx_status = 'SUCCEEDED'
),
  
burns AS (
SELECT
  tx_id,
  
  event_attributes:protocol_fee[0]:amount / POW(10,6) AS protocol_fee_amount,
  event_attributes:protocol_fee[0]:denom::string AS protocol_fee_currency,
  protocol_fee_amount * o.price AS procotol_fee_amount_usd,
  
  event_attributes:burn_amount[0]:amount / POW(10,6) AS burn_amount,
  burn_amount * i.price AS burn_amount_usd,
  event_attributes:burn_amount[0]:denom::string AS burn_currency
FROM {{source('silver_terra', 'msg_events')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.protocol_fee_currency = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.burn_currency = i.currency  

WHERE event_attributes:burn_amount IS NOT NULL 
  AND tx_id IN(SELECT DISTINCT tx_id 
                FROM msgs)
  AND tx_status = 'SUCCEEDED'
),
  
withdraw as (
SELECT
  tx_id,
  
  event_attributes:tax_amount[0]:amount /POW(10,6) AS tax_amount,
  tax_amount * o.price AS tax_amount_usd,
  event_attributes:tax_amount[0]:denom::string AS tax_currency,
  
  event_attributes:withdraw_amount[0]:amount /POW(10,6) AS withdraw_amount,
  withdraw_amount * i.price AS withdraw_amount_usd,
  event_attributes:withdraw_amount[0]:denom::string AS withdraw_currency
FROM {{source('silver_terra', 'msg_events')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.tax_currency = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.withdraw_currency = i.currency  

WHERE event_attributes:withdraw_amount IS NOT NULL 
  AND tx_id IN(SELECT DISTINCT tx_id 
                FROM msgs)
  AND tx_status = 'SUCCEEDED'
)

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  collateral_id,
  sender,
  protocol_fee_amount,
  procotol_fee_amount_usd,
  protocol_fee_currency,
  tax_amount,
  tax_amount_usd,
  tax_currency,
  burn_amount,
  burn_amount_usd,
  burn_currency,
  withdraw_amount,
  withdraw_amount_usd,
  withdraw_currency,
  contract_address,
  contract_label
FROM msgs m

JOIN burns b
  ON m.tx_id = b.tx_id
  
JOIN withdraw w
  ON m.tx_id = w.tx_id
