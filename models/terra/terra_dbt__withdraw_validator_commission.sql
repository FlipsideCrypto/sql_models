{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'reward']
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
  REGEXP_REPLACE(msg_value:validator_address,'\"','') as validator_address
FROM {{source('terra', 'terra_msgs')}} 
WHERE msg_module = 'distribution' 
  AND msg_type = 'distribution/MsgWithdrawValidatorCommission' 
  {% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}