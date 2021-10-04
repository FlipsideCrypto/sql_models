{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'polygon_silver', 'polygon_transactions','polygon']
  )
}}

select 
  system_created_at,
  chain_id,
  block_id,
  block_timestamp,
  fee,
  from_address,
  gas_limit,
  gas_price,
  gas_used,
  input_method,
  native_value,
  nonce,
  success,
  to_address,
  tx_id, 
  tx_position
from {{ ref('polygon_dbt__transactions')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_polygon', 'transactions')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, tx_id order by block_id desc, system_created_at desc)) = 1