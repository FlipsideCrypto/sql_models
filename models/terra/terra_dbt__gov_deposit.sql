{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'gov']
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
  REGEXP_REPLACE(msg_value:depositor,'\"','') as depositor,
  REGEXP_REPLACE(msg_value:proposal_id,'\"','') as proposal_id,
  REGEXP_REPLACE(msg_value:amount[0]:amount/ POW(10,6),'\"','') as amount,
  REGEXP_REPLACE(msg_value:amount[0]:denom,'\"','') as currency
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_module = 'gov' 
  AND msg_type = 'gov/MsgDeposit'
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}