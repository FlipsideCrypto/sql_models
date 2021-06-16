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
WHERE
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '1 days'
{% else %}
  block_timestamp >= getdate() - interval '9 months'
{% endif %}