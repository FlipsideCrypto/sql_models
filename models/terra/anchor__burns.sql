{{ config(
    materialized = 'incremental',
    unique_key = 'block_id || tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra', 'anchor', 'burns']
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

)

SELECT 
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender::string as sender,
  msg_value:execute_msg:send:amount / POW(10,6) as amount,
  amount * price AS amount_usd,
  msg_value:contract::string as currency,
  msg_value:execute_msg:send:contract::string as contract_address,
  l.address_name AS contract_label
FROM {{source('silver_terra', 'msgs')}} m

LEFT OUTER JOIN prices o
 ON date_trunc('hour', block_timestamp) = o.hour
 AND msg_value:contract::string = o.currency 

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as l
ON msg_value:execute_msg:send:contract::string = l.address

WHERE msg_value:execute_msg:send:msg:unbond IS NOT NULL 
  AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{source('silver_terra', 'terra_msgs')}})
  {% endif %}