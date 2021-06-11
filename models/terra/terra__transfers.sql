{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'transfers']
  )
}}

WITH transfers as (
WITH inputs as(
  SELECT 
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,  
    tx_id,
    tx_type,
    msg_type, 
    vm.value:index as event_index,
    REGEXP_REPLACE(vm.value:address,'\"','') as event_from,
    ve.value:amount/ pow(10,6) as event_amount,
    REGEXP_REPLACE(ve.value:denom,'\"','') as event_currency,
    msg_value
  FROM {{source('terra', 'terra_msgs')}}
  , lateral flatten(input => tendermintBankMsgMultiSendAddIndex(msg_value):inputs) vm
  , lateral flatten(input => vm.value:coins) ve
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
    blockchain,
    chain_id,
    block_id,
    block_timestamp,  
    tx_id,
    tx_type,
    vm.value:index as event_index,
    REGEXP_REPLACE(vm.value:address,'\"','') as event_to,
    ve.value:amount/ pow(10,6) as event_amount,
    REGEXP_REPLACE(ve.value:denom,'\"','') as event_currency
  FROM {{source('terra', 'terra_msgs')}}
  , lateral flatten(input => tendermintBankMsgMultiSendAddIndex(msg_value):outputs) vm
  , lateral flatten(input => vm.value:coins) ve
  WHERE msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
    {% if is_incremental() %}
      AND block_timestamp >= getdate() - interval '1 days'
    {% else %}
      AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
)
SELECT 
  i.blockchain,
  i.chain_id,
  i.block_id,
  i.block_timestamp,  
  i.tx_id,
  i.tx_type,
  i.event_from,
  o.event_to,
  i.event_amount,
  i.event_currency
FROM inputs i

JOIN outputs o 
  ON i.tx_id = o.tx_id
  AND i.event_index = o.event_index
  
UNION

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type, 
  REGEXP_REPLACE(msg_value:from_address,'\"','') as event_from,
  REGEXP_REPLACE(msg_value:to_address,'\"','') as event_to,
  msg_value:amount[0]:amount / pow(10,6) as event_amount,
  REGEXP_REPLACE(msg_value:amount[0]:denom,'\"','') as event_currency
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'bank'
  AND msg_type = 'bank/MsgSend'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}),

prices as (
  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price_usd
    FROM {{ ref('terra__oracle_prices')}} 
    {% if is_incremental() %}
       AND block_timestamp >= getdate() - interval '1 days'
    {% else %}
       AND block_timestamp >= getdate() - interval '9 months'
    {% endif %} 
    GROUP BY 1,2,3
)

SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type, 
  event_from,
  from_labels.l1_label as event_from_label_type,
  from_labels.l2_label as event_from_label_subtype,
  from_labels.project_name as event_from_address_label,
  from_labels.address_name as event_from_address_name,
  event_to,
  to_labels.l1_label as event_to_label_type,
  to_labels.l2_label as event_to_label_subtype,
  to_labels.project_name as event_to_address_label,
  to_labels.address_name as event_to_address_name,
  event_amount,
  event_amount * price_usd as event_amount_usd,
  symbol as event_currency
FROM transfers t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND t.event_currency = o.currency 

LEFT OUTER JOIN {{source('shared','udm_address_labels')}} as from_labels
ON event_from = from_labels.address

LEFT OUTER JOIN {{source('shared','udm_address_labels')}} as to_labels
ON event_to = to_labels.address

