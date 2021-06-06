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
  fee[0]:denom::string AS fee_currency,
  fee[0]:amount AS fee_amount,
  gas_used,
  gas_wanted
FROM {{source('terra', 'terra_transactions')}}