{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'staking']
  )
}}

WITH staking AS (
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
FROM {{source('silver_terra', 'msgs')}} 
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgBeginRedelegate'
  AND tx_status = 'SUCCEEDED'
  {% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
)

SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  delegator_address,
  validator_src_address,
  validator_dst_address,
  event_amount,
  event_currency
FROM staking
