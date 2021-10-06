{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'mirror', 'reward_claims']
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

prices_backup AS (

SELECT 
  date_trunc('day', block_timestamp) as day,
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
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:contract::string as contract_address
FROM terra.msgs 
WHERE msg_value:execute_msg:withdraw_voting_rewards IS NOT NULL

{% if is_incremental() %}
  AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
{% endif %}
  
),

events AS (

SELECT 
  m.tx_id,
  event_attributes:"1_amount" / POW(10,6) as claim_amount,
  event_attributes:"1_contract_address"::string as claim_currency
FROM terra.msg_events e
  
JOIN msgs m 
  ON m.tx_id = e.tx_id

WHERE event_attributes:"0_action" = 'withdraw'

{% if is_incremental() %}
  AND e.block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'msgs')}})
{% endif %}

)

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  claim_amount,
  claim_amount * coalesce(p.price,pb.price) AS claim_amount_usd,
  claim_currency,
  contract_address,
  l.address_name AS contract_label
FROM msgs m

JOIN events e 
  ON m.tx_id = e.tx_id

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON contract_address = l.address

LEFT OUTER JOIN prices p
 ON date_trunc('hour', m.block_timestamp) = p.hour
 AND claim_currency = p.currency 

LEFT OUTER JOIN prices_backup pb
 ON date_trunc('day', m.block_timestamp) = pb.day
 AND claim_currency = pb.currency 
