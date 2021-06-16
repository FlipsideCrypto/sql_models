{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'swap']
  )
}}

WITH msgs AS(
SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp, 
  tx_status,
  tx_id, 
  msg_index,
  msg_type,
  REGEXP_REPLACE(msg_value:trader,'\"','') as trader,
  REGEXP_REPLACE(msg_value:ask_denom,'\"','') as ask_currency,
  REGEXP_REPLACE(msg_value:offer_coin:amount,'\"','') as offer_amount,
  REGEXP_REPLACE(msg_value:offer_coin:denom,'\"','') as offer_currency,
  msg_value
FROM {{source('terra', 'terra_msgs')}}
WHERE msg_module = 'market' 
  AND msg_type = 'market/MsgSwap' 
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),

events_transfer as(  
SELECT tx_id,
       event_type,
       event_attributes,
       msg_index,
       REGEXP_REPLACE(event_attributes:"0_sender",'\"','') as "0_sender",
       REGEXP_REPLACE(event_attributes:"0_recipient",'\"','') as "0_recipient",
       REGEXP_REPLACE(event_attributes:"0_amount"[0]:amount,'\"','') as "0_amount",
       REGEXP_REPLACE(event_attributes:"0_amount"[0]:denom,'\"','') as "0_amount_currency",
       REGEXP_REPLACE(event_attributes:"1_sender",'\"','') as "1_sender",
       REGEXP_REPLACE(event_attributes:"1_recipient",'\"','') as "1_recipient",
       REGEXP_REPLACE(event_attributes:"1_amount"[0]:amount,'\"','') as "1_amount",
       REGEXP_REPLACE(event_attributes:"1_amount"[0]:denom,'\"','') as "1_amount_currency"
FROM {{source('terra', 'terra_msg_events')}}
WHERE event_type = 'transfer'
  AND msg_type = 'market/MsgSwap'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),

fees as(
SELECT 
  tx_id,
  event_type,
  msg_index,
  event_attributes:swap_fee[0]:amount as swap_fee_amount,
  REGEXP_REPLACE(event_attributes:swap_fee[0]:denom,'\"','') as swap_fee_currency
FROM {{source('terra', 'terra_msg_events')}}
WHERE event_type = 'swap'
  AND msg_type = 'market/MsgSwap'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),

contract as (
select 
  tx_id,
  event_type,
  event_attributes,
  msg_index,
  REGEXP_REPLACE(event_attributes:contract_address,'\"','') as contract_address,
  event_attributes as contract_attr
FROM {{source('terra', 'terra_msg_events')}}
WHERE event_type = 'execute_contract'
  AND msg_type = 'wasm/MsgExecuteContract'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),

prices as (
    SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(luna_exchange_rate) as luna_exchange_rate,
      avg(price_usd) as price_usd,
      avg(luna_usd_price) as luna_usd_price
    FROM {{ ref('terra__oracle_prices')}} 
    WHERE
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '3 days'
  {% else %}
    block_timestamp >= getdate() - interval '12 months'
  {% endif %}
    GROUP BY 1,2,3
)



SELECT 
  m.blockchain,
  m.chain_id,
  m.block_id,
  m.block_timestamp, 
  m.tx_status,
  m.tx_id, 
  f.swap_fee_amount / POW(10,6) as swap_fee_amount,
  f.swap_fee_amount / POW(10,6) * fe.price_usd as swap_fee_amount_usd,
  fe.symbol as swap_fee_currency,
  m.trader, 
  -- trader_labels.l1_label as trader_label_type,
  -- trader_labels.l2_label as trader_label_subtype,
  -- trader_labels.project_name as trader_address_label,
  -- trader_labels.address_name as trader_address_name,
  aa.symbol as ask_currency,
  m.offer_amount / POW(10,6) as offer_amount,
  m.offer_amount / POW(10,6) * oo.price_usd as offer_amount_usd,
  oo.symbol as offer_currency,
  -- et."0_sender" as sender,
  -- sender_labels.l1_label as sender_label_type,
  -- sender_labels.l2_label as sender_label_subtype,
  -- sender_labels.project_name as sender_address_label,
  -- sender_labels.address_name as sender_address_name,
  -- et."0_recipient" as receiver,
  -- receiver_labels.l1_label as receiver_label_type,
  -- receiver_labels.l2_label as receiver_label_subtype,
  -- receiver_labels.project_name as receiver_address_label,
  -- receiver_labels.address_name as receiver_address_name,
  et."0_amount" / POW(10,6) as token_0_amount,
  token_0_amount * z.price_usd as token_0_amount_usd,
  z.symbol as token_0_currency,
  et."1_amount"/ POW(10,6) as token_1_amount,
  token_1_amount * o.price_usd as token_1_amount_usd,
  o.symbol as token_1_currency,
  z.price_usd as price0_usd,
  o.price_usd as price1_usd,
  token_0_currency || ' to ' || token_1_currency as swap_pair,
  c.contract_address
FROM msgs m

LEFT OUTER JOIN events_transfer et 
 ON m.tx_id = et.tx_id 
 AND m.msg_index = et.msg_index

LEFT OUTER JOIN fees f 
 ON m.tx_id = f.tx_id 
 AND m.msg_index = f.msg_index
 
LEFT OUTER JOIN contract c
 ON m.tx_id = c.tx_id 
 AND m.msg_index = c.msg_index

LEFT OUTER JOIN prices z
 ON date_trunc('hour', m.block_timestamp) = z.hour
 AND et."0_amount_currency" = z.currency

LEFT OUTER JOIN prices o
 ON date_trunc('hour', m.block_timestamp) = o.hour
 AND et."1_amount_currency" = o.currency 

LEFT OUTER JOIN prices fe
 ON date_trunc('hour', m.block_timestamp) = fe.hour
 AND f.swap_fee_currency = fe.currency 

LEFT OUTER JOIN prices oo
 ON date_trunc('hour', m.block_timestamp) = oo.hour
 AND m.offer_currency = oo.currency 
 
LEFT OUTER JOIN prices aa
 ON date_trunc('hour', m.block_timestamp) = aa.hour
 AND m.ask_currency = aa.currency 

-- LEFT OUTER JOIN {{source('shared','udm_address_labels')}} as trader_labels
-- ON m.trader = trader_labels.address

-- LEFT OUTER JOIN {{source('shared','udm_address_labels')}} as sender_labels
-- ON et."0_sender" = sender_labels.address

-- LEFT OUTER JOIN {{source('shared','udm_address_labels')}} as receiver_labels
-- ON et."0_recipient" = receiver_labels.address

WHERE tx_status = 'SUCCEEDED'