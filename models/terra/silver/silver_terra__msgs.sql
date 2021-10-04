{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id || msg_index', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id', 'tx_id'],
    tags=['snowflake', 'terra_silver', 'terra_msgs']
  )
}}

select 
  system_created_at,
  blockchain,
  block_id,
  block_timestamp,
  chain_id,
  tx_id,
  tx_type,
  tx_status,
  tx_module,
  msg_index,
  msg_type,
  msg_module,
  msg_value
from {{ ref('terra_dbt__msgs')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_terra', 'msgs')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id, msg_index order by system_created_at desc)) = 1
