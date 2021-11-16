{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'anchor', 'deposits']
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

msgs AS (

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_index,
  msg_value:sender::string as sender,
  msg_value:coins[0]:amount / POW(10,6) as deposit_amount,
  deposit_amount * price AS deposit_amount_usd,
  msg_value:coins[0]:denom::string as deposit_currency,
  msg_value:contract::string as contract_address,
  l.address_name as contract_label
FROM {{ref('silver_terra__msgs')}} m

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:contract::string = l.address

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND msg_value:coins[0]:denom::string = o.currency 

WHERE msg_value:execute_msg:deposit_stable IS NOT NULL 
  AND msg_value:contract::string = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

),

events AS (

SELECT 
  tx_id,
  msg_index,
  event_attributes:mint_amount / POW(10,6) as mint_amount,
  mint_amount * price AS mint_amount_usd,
  event_attributes:"1_contract_address"::string as mint_currency
FROM {{ref('silver_terra__msg_events')}}

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND event_attributes:"1_contract_address"::string = o.currency 

WHERE event_type = 'from_contract'
  AND tx_id IN(SELECT tx_id FROM msgs)
  AND event_attributes:"0_action"::string = 'deposit_stable'
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ref('silver_terra__msgs')}})
  {% endif %}

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