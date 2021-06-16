{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
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
  fee[0]:denom::string AS fee_currency,
  fee[0]:amount AS fee_amount,
  gas_used,
  gas_wanted
FROM {{source('terra', 'terra_transactions')}}
WHERE 
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '1 days'
{% else %}
  block_timestamp >= getdate() - interval '9 months'
{% endif %}