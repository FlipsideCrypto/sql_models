{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver', 'terra_transactions']
  )
}}

select 
  system_created_at,
  blockchain,
  block_id,
  block_timestamp,
  chain_id,
  codespace,
  tx_id,
  tx_type,
  tx_module,
  tx_status,
  tx_status_msg,
  tx_code,
  fee,
  gas_wanted,
  gas_used
from {{ ref('terra_dbt__transactions')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_terra', 'transactions')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id order by system_created_at desc)) = 1