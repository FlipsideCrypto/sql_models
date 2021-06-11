{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
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
  REGEXP_REPLACE(msg_value:delegator_address,'\"','') as delegator_address,
  REGEXP_REPLACE(msg_value:validator_src_address,'\"','') as validator_src_address,
  REGEXP_REPLACE(msg_value:validator_dst_address,'\"','') as validator_dst_address,
  REGEXP_REPLACE(msg_value:amount:amount / POW(10,6),'\"','') as event_amount,
  REGEXP_REPLACE(msg_value:amount:denom,'\"','') as event_currency
FROM {{source('terra', 'terra_msgs')}} 
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgBeginRedelegate'
  {% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}