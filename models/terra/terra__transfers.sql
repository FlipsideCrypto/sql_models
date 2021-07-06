{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'transfers']
  )
}}


WITH prices as (
  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price_usd
    FROM {{ ref('terra__oracle_prices')}} 
    GROUP BY 1,2,3
),

inputs as(
  SELECT 
    blockchain, 
    chain_id, 
    tx_status, 
    block_id, 
    block_timestamp, 
    tx_id, 
    msg_type, 
    a.value:address::string as event_from, 
    a.value:coins[0]:amount / POW(10,6) as event_amount, 
    a.value:coins[0]:denom::string as event_currency, 
    a.index as input_index
  FROM {{source('silver_terra', 'msgs')}}
  , lateral flatten(input => msg_value:inputs) a
  WHERE msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
    
    {% if is_incremental() %}
      AND block_timestamp >= getdate() - interval '1 days'
    {% else %}
      AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),

outputs as(
  SELECT 
    tx_id, 
    a.value:address::string as event_to, 
    a.index as output_index
  FROM {{source('silver_terra', 'msgs')}}
  , lateral flatten(input => msg_value:outputs) a
  WHERE msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
   
    {% if is_incremental() %}
      AND block_timestamp >= getdate() - interval '1 days'
    {% else %}
      AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),

transfers as(
SELECT 
  blockchain,
  chain_id, 
  tx_status, 
  block_id, 
  block_timestamp, 
  i.tx_id, 
  msg_type, 
  event_from, 
  event_to, 
  event_amount, 
  event_currency
FROM inputs i

JOIN outputs o 
  ON i.tx_id = o.tx_id
  AND i.input_index = o.output_index
  
UNION

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type, 
  msg_value:from_address::string as event_from,
  msg_value:to_address::string as event_to,
  msg_value:amount[0]:amount / pow(10,6) as event_amount,
  msg_value:amount[0]:denom::string as event_currency
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_module = 'bank'
  AND msg_type = 'bank/MsgSend'

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
)


SELECT
  t.blockchain,
  t.chain_id,
  t.tx_status,
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.msg_type, 
  t.event_from,
  from_labels.l1_label as event_from_label_type,
  from_labels.l2_label as event_from_label_subtype,
  from_labels.project_name as event_from_address_label,
  from_labels.address_name as event_from_address_name,
  t.event_to,
  to_labels.l1_label as event_to_label_type,
  to_labels.l2_label as event_to_label_subtype,
  to_labels.project_name as event_to_address_label,
  to_labels.address_name as event_to_address_name,
  t.event_amount,
  t.event_amount * price_usd as event_amount_usd,
  o.symbol as event_currency
FROM transfers t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_currency = o.currency 

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as from_labels
ON event_from = from_labels.address

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as to_labels
ON event_to = to_labels.address