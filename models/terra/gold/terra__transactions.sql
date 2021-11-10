{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'transactions', 'terra']
) }}

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
  fee,
  gas_used,
  gas_wanted
FROM
  {{ ref('silver_terra__transactions') }}
