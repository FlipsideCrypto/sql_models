{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'transactions']
  )
}}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_type,
  tx_status,
  tx_status_msg,
  tx_code,
  tx_module,
  codespace,
  gas_used,
  gas_wanted
FROM {{source('terra', 'terra_transactions')}}
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}