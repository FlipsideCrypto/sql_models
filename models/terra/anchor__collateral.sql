{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'anchor', 'collateral']
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
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
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
  'withdraw' as action,
  msg_value:sender::string AS sender,
  msg_value:execute_msg:send:contract::string AS contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

WHERE msg_value:execute_msg:withdraw_collateral IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}

),

events AS (

  SELECT 
  tx_id,
  event_attributes:collaterals[0]:amount / POW(10,6) as amount,
  amount * price AS amount_usd,
  event_attributes:collaterals[0]:denom::string as currency
FROM {{source('silver_terra', 'msg_events')}} m

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND event_attributes:collaterals[0]:denom::string = o.currency 

WHERE tx_id IN(SELECT tx_id FROM msgs)
  AND event_type = 'from_contract'
  AND msg_index = 0
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}
  
)  

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  action as event_type,
  sender,
  amount,
  amount_usd,
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
  'provide' as event_type,
  msg_value:sender::string AS sender,
  msg_value:execute_msg:send:amount / POW(10,6) as amount,
  amount * price AS amount_usd,
  msg_value:contract::string as currency,
  msg_value:execute_msg:send:contract::string AS contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND msg_value:contract::string = o.currency 

WHERE msg_value:execute_msg:send:msg:deposit_collateral IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}