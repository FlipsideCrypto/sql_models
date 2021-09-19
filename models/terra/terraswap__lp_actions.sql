{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'terraswap', 'lp']
) }}

WITH prices as (

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

provide_msgs AS (

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'provide_liquidity' as event_type,
  msg_value:sender::string as sender,
  msg_value:contract::string as pool_address,
  l.address_name AS pool_name
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

WHERE msg_value:execute_msg:provide_liquidity IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}

),

provide_events AS (
SELECT
  tx_id,
  
  event_attributes:assets[0]:amount / POW(10,6) AS token_0_amount,
  token_0_amount * o.price AS token_0_amount_usd,
  event_attributes:assets[0]:denom::string AS token_0_currency,
  
  event_attributes:assets[1]:amount / POW(10,6) AS token_1_amount,
  token_1_amount * i.price AS token_1_amount_usd,
  event_attributes:assets[1]:denom::string AS token_1_currency,
  
  event_attributes:share / POW(10,6) AS lp_share_amount,
  CASE 
    WHEN event_attributes:"2_contract_address"::string = 'terra17yap3mhph35pcwvhza38c2lkj7gzywzy05h7l0' THEN event_attributes:"4_contract_address"::string 
    ELSE event_attributes:"2_contract_address"::string 
  END AS lp_pool_address,
  l.address_name AS lp_pool_name
FROM {{source('silver_terra', 'msg_events')}} t

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON event_attributes:"2_contract_address"::string = l.address

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as r
ON event_attributes:"4_contract_address"::string = r.address

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_attributes:assets[0]:denom::string = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.event_attributes:assets[1]:denom::string = i.currency  

WHERE msg_index = 1
  AND tx_id IN(SELECT DISTINCT tx_id FROM provide_msgs)
  AND event_type = 'from_contract'

  {% if is_incremental() %}
    AND t.block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}

),

withdraw_msgs AS (
  
SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'withdraw_liquidity' as event_type,
  msg_value:sender::string as sender,
  msg_value:contract::string as lp_pool_address,
  l.address_name AS lp_pool_name,
  msg_value:execute_msg:send:contract::string as pool_address,
  p.address_name AS pool_name
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as p
ON msg_value:contract::string = p.address

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

WHERE msg_value:execute_msg:send:msg:withdraw_liquidity IS NOT NULL
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND m.block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}
  
),

withdraw_events AS (

SELECT 
  tx_id,
  
  event_attributes:refund_assets[0]:amount / POW(10,6) AS token_0_amount,
  token_0_amount * o.price AS token_0_amount_usd,
  event_attributes:refund_assets[0]:denom::string AS token_0_currency,
  
  event_attributes:refund_assets[1]:amount / POW(10,6) AS token_1_amount,
  token_1_amount * i.price AS token_1_amount_usd,
  event_attributes:refund_assets[1]:denom::string AS token_1_currency,
  
  event_attributes:withdrawn_share / POW(10,6) as lp_share_amount
FROM {{source('silver_terra', 'msg_events')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_attributes:refund_assets[0]:denom::string = o.currency 

LEFT OUTER JOIN prices i
 ON date_trunc('hour', t.block_timestamp) = i.hour
 AND t.event_attributes:refund_assets[1]:denom::string = i.currency  

WHERE tx_id IN(SELECT tx_id FROM provide_msgs)
  AND event_type = 'from_contract'
  AND event_attributes:refund_assets IS NOT NULL 

  {% if is_incremental() %}
    AND t.block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}

)

-- Provide Liquidity 
SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  event_type,
  sender,
  token_0_amount,
  token_0_amount_usd,
  token_0_currency,
  token_1_amount,
  token_1_amount_usd,
  token_1_currency,
  pool_address,
  pool_name,
  lp_share_amount,
  lp_pool_address,
  lp_pool_name
FROM provide_msgs m 

JOIN provide_events e 
  ON m.tx_id = e.tx_id

UNION 

-- Remove Liquidity 
SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  event_type,
  sender,
  token_0_amount,
  token_0_amount_usd,
  token_0_currency,
  token_1_amount,
  token_1_amount_usd,
  token_1_currency,
  pool_address,
  pool_name,
  lp_share_amount,
  lp_pool_address,
  lp_pool_name
FROM withdraw_msgs m 

JOIN withdraw_events e 
  ON m.tx_id = e.tx_id