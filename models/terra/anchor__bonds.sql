{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'anchor', 'bonds']
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
  msg_value:sender::string AS sender,
  msg_value:coins[0]:amount / POW(10,6) AS bonded_amount,
  bonded_amount * price AS bonded_amount_usd,
  msg_value:coins[0]:denom::string as bonded_currency,
  msg_value:execute_msg:bond:validator::string AS validator,
  msg_value:contract::string AS contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND msg_value:coins[0]:denom::string = o.currency 

WHERE msg_value:execute_msg:bond IS NOT NULL
  AND tx_status = 'SUCCEEDED'
  
  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
  {% endif %}

),

events AS (

SELECT 
  tx_id,
  event_attributes:"1_amount" / POW(10,6) AS minted_amount,
  minted_amount * price AS minted_amount_usd, 
  event_attributes:"1_contract_address"::string as minted_currency
FROM {{source('silver_terra', 'msg_events')}}

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND event_attributes:"1_contract_address"::string = o.currency 

WHERE tx_id IN(SELECT tx_id FROM msgs)
  AND event_type = 'from_contract'
  AND tx_status = 'SUCCEEDED'
  AND minted_currency IS NOT NULL
  AND minted_amount IS NOT NULL

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