{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'mirror', 'mirror_close_collateral']
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
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
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
  msg_value:execute_msg:send:msg:burn:position_idx as collateral_id,
  msg_value:sender::string as sender,
  msg_value:contract::string as contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

WHERE msg_value:execute_msg:send:msg:burn IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
  {% endif %}
),
  
burns AS (
SELECT
  tx_id,
  event_attributes:burn_amount[0]:amount / POW(10,6) AS burn_amount,
  burn_amount * i.price AS burn_amount_usd,
  event_attributes:burn_amount[0]:denom::string AS burn_currency,

  event_attributes:protocol_fee[0]:amount / POW(10,6) AS protocol_fee_amount,
  protocol_fee_amount * f.price AS procotol_fee_amount_usd,
  event_attributes:protocol_fee[0]:denom::string AS protocol_fee_currency,
FROM {{source('silver_terra', 'msg_events')}} t

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.event_attributes:burn_amount[0]:denom::string = i.currency  

LEFT OUTER JOIN prices f
 ON date_trunc('hour', t.block_timestamp) = f.hour
 AND t.event_attributes:protocol_fee[0]:denom::string = f.currency 

WHERE event_attributes:burn_amount IS NOT NULL 
  AND tx_id IN(SELECT DISTINCT tx_id 
                FROM msgs)
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
  {% endif %}
),
  
withdraw as (
SELECT
  tx_id,
  
  event_attributes:tax_amount[0]:amount /POW(10,6) AS tax_amount,
  tax_amount * o.price AS tax_amount_usd,
  event_attributes:tax_amount[0]:denom::string AS tax_currency,

  event_attributes:protocol_fee[0]:amount / POW(10,6) AS protocol_fee_amount,
  protocol_fee_amount * f.price AS procotol_fee_amount_usd,
  event_attributes:protocol_fee[0]:denom::string AS protocol_fee_currency,
  
  event_attributes:withdraw_amount[0]:amount /POW(10,6) AS withdraw_amount,
  withdraw_amount * i.price AS withdraw_amount_usd,
  event_attributes:withdraw_amount[0]:denom::string AS withdraw_currency
FROM {{source('silver_terra', 'msg_events')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_attributes:tax_amount[0]:denom::string = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.event_attributes:withdraw_amount[0]:denom::string = i.currency  

LEFT OUTER JOIN prices f
 ON date_trunc('hour', t.block_timestamp) = f.hour
 AND t.event_attributes:protocol_fee[0]:denom::string = f.currency 

WHERE event_attributes:withdraw_amount IS NOT NULL 
  AND tx_id IN(SELECT DISTINCT tx_id 
                FROM msgs)
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
  {% endif %}
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
  procotol_fee_amount_usd,
  protocol_fee_currency,
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
