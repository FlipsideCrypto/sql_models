{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id || msg_index || event_index || event_type', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id', 'tx_id'],
    tags=['snowflake', 'terra_silver', 'terra_msg_events']
  )
}}

select 
  system_created_at,
  blockchain,
  block_id,
  block_timestamp,
  chain_id,
  tx_id,
  tx_module,
  tx_status,
  tx_type,
  msg_index,
  msg_type,
  msg_module,
  event_type,
  event_index,
  event_attributes
from {{ ref('terra_dbt__msg_events')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_terra', 'msg_events')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id, msg_index, event_index, event_type order by system_created_at desc)) = 1
