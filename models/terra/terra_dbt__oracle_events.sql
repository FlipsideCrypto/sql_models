{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'oracle']
  )
}}

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  REGEXP_REPLACE(vm.value:denom,'\"','') as currency,
  vm.value:rate as rate,
  REGEXP_REPLACE(msg_value:feeder,'\"','') as feeder,
  REGEXP_REPLACE(msg_value:salt,'\"','') as salt,
  REGEXP_REPLACE(msg_value:validator,'\"','') as validator
FROM {{source('terra', 'terra_msgs')}} 
, lateral flatten(input => msg_value:exchange_rates) vm

WHERE msg_module = 'oracle' 
  AND msg_type = 'oracle/MsgAggregateExchangeRateVote'
  {% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  REGEXP_REPLACE(msg_value:denom,'\"','') as currency,
  msg_value:exchange_rate as rate,
  REGEXP_REPLACE(msg_value:feeder,'\"','') as feeder,
  REGEXP_REPLACE(msg_value:salt,'\"','') as salt,
  REGEXP_REPLACE(msg_value:validator,'\"','') as validator
FROM {{source('terra', 'terra_msgs')}}  
WHERE msg_module = 'oracle' 
  AND msg_type = 'oracle/MsgExchangeRateVote'
  {% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}