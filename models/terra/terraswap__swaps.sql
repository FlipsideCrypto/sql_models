{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'terraswap', 'swap']
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

msgs as (
-- native to non-native/native
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:contract::string as pool_address
FROM {{ref('silver_terra__msgs')}}
WHERE msg_value:execute_msg:swap IS NOT NULL
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
  

 UNION 
 
-- non-native to native
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:contract::string as pool_address
FROM {{ref('silver_terra__msgs')}}

WHERE msg_value:execute_msg:send:msg:swap IS NOT NULL
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
),

events as (
SELECT
  tx_id,
  as_number(event_attributes:tax_amount) / POW(10,6) as tax_amount,
  event_attributes:commission_amount::numeric / POW(10,6) as commission_amount,
  event_attributes:offer_amount::numeric / POW(10,6) as offer_amount,
  event_attributes:offer_asset::string as offer_currency,
  event_attributes:return_amount::numeric / POW(10,6) as return_amount,
  event_attributes:ask_asset::string as return_currency
FROM {{ref('silver_terra__msg_events')}}

WHERE event_type = 'from_contract'
  AND tx_id IN(SELECT DISTINCT tx_id 
 	  		    FROM msgs )

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}
)


SELECT 
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  tax_amount,
  commission_amount,
  offer_amount,
  offer_amount * o.price AS offer_amount_usd,
  offer_currency,
  return_amount,
  return_amount * r.price AS return_amount_usd,
  return_currency,
  pool_address,
  l.address_name AS pool_name
FROM msgs m 

JOIN events e 
  ON m.tx_id = e.tx_id

LEFT OUTER JOIN prices o
 ON date_trunc('hour', m.block_timestamp) = o.hour
 AND e.offer_currency = o.currency 

LEFT OUTER JOIN prices r
 ON date_trunc('hour', m.block_timestamp) = r.hour
 AND e.return_currency = r.currency  

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} l
  ON pool_address = address