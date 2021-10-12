{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'mirror', 'short_farm']
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

prices_backup AS (

  SELECT 
      date_trunc('day', block_timestamp) as day,
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

tx AS (

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value
FROM {{ref('silver_terra__msgs')}}
WHERE msg_value:execute_msg:send:msg:burn IS NOT NULL
  AND msg_value:execute_msg:send:contract::string = 'terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

),

event_tx AS (

SELECT 
  block_timestamp,
  tx_id,
  event_attributes
FROM {{ref('silver_terra__msg_events')}}
WHERE tx_id IN(select tx_id from tx)
  AND event_type = 'from_contract'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

),

msgs as (

SELECT
  t.blockchain,
  chain_id,
  block_id,
  block_timestamp, 
  tx_id,
  msg_value:execute_msg:send:msg:burn:position_idx as collateral_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:contract::string as contract_address,
  l.address_name AS contract_label
FROM tx t

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

),

withdraw_events AS (

SELECT 
  tx_id,
  
  event_attributes:withdraw_amount[0]:amount / POW(10,6) AS withdraw_amount,
  withdraw_amount * coalesce(o.price,o_b.price) AS withdraw_amount_usd,
  event_attributes:withdraw_amount[0]:denom::string AS withdraw_currency,
  
  event_attributes:unlocked_amount[0]:amount / POW(10,6) AS unlocked_amount,
  unlocked_amount * coalesce(i.price,i_b.price) AS unlocked_amount_usd,
  event_attributes:unlocked_amount[0]:denom::string AS unlocked_currency,
  
  (event_attributes:"0_tax_amount"[0]:amount + event_attributes:"1_tax_amount"[0]:amount) / POW(10,6) AS tax_amount,
  tax_amount * coalesce(a.price,a_b.price) AS tax_amount_usd,
  event_attributes:"0_tax_amount"[0]:denom::string AS tax_currency
  
FROM event_tx t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_attributes:withdraw_amount[0]:denom::string = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.event_attributes:unlocked_amount[0]:denom::string = i.currency  

LEFT OUTER JOIN prices a
 ON date_trunc('hour', t.block_timestamp) = a.hour
 AND t.event_attributes:"0_tax_amount"[0]:denom::string = a.currency  

LEFT OUTER JOIN prices_backup o_b
 ON date_trunc('day', t.block_timestamp) = o_b.day
 AND t.event_attributes:withdraw_amount[0]:denom::string = o_b.currency 

LEFT OUTER JOIN prices_backup i_b
 ON date_trunc('day', t.block_timestamp) = i_b.day
 AND t.event_attributes:unlocked_amount[0]:denom::string = i_b.currency  

LEFT OUTER JOIN prices_backup a_b
 ON date_trunc('day', t.block_timestamp) = a_b.day
 AND t.event_attributes:"0_tax_amount"[0]:denom::string = a_b.currency  

WHERE event_attributes:withdraw_amount IS NOT NULL 

),

burn_events AS (

SELECT 
  tx_id,
  
  event_attributes:burn_amount[0]:amount / POW(10,6) AS burn_amount,
  burn_amount * coalesce(o.price,o_b.price) AS burn_amount_usd,
  event_attributes:burn_amount[0]:denom::string AS burn_currency,  
  
  event_attributes:protocol_fee[0]:amount / POW(10,6) AS protocol_fee_amount,
  protocol_fee_amount * coalesce(i.price,i_b.price) AS protocol_fee_amount_usd,
  event_attributes:protocol_fee[0]:denom::string AS protocol_fee_currency
FROM event_tx t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_attributes:burn_amount[0]:denom::string = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.event_attributes:protocol_fee[0]:denom::string = i.currency  

LEFT OUTER JOIN prices_backup o_b
 ON date_trunc('day', t.block_timestamp) = o_b.day
 AND t.event_attributes:burn_amount[0]:denom::string = o_b.currency 

LEFT OUTER JOIN prices_backup i_b
 ON date_trunc('day', t.block_timestamp) = i_b.day
 AND t.event_attributes:protocol_fee[0]:denom::string = i_b.currency  

WHERE event_attributes:burn_amount IS NOT NULL
  
)

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  collateral_id,
  sender,
  tax_amount,
  tax_amount_usd,
  tax_currency,
  protocol_fee_amount,
  protocol_fee_amount_usd,
  protocol_fee_currency,
  burn_amount,
  burn_amount_usd,
  burn_currency,
  withdraw_amount,
  withdraw_amount_usd,
  withdraw_currency,
  unlocked_amount,
  unlocked_amount_usd,
  unlocked_currency,
  contract_address,
  contract_label
FROM msgs m

JOIN withdraw_events w 
  ON m.tx_id = w.tx_id

JOIN burn_events b 
  ON m.tx_id = b.tx_id

WHERE unlocked_amount IS NOT NULL