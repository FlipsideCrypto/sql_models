{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id || msg_index || event_index || event_type', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id', 'tx_id'],
    tags=['snowflake', 'terra_silver_2', 'terra_msg_events']
  )
}}

select *
from {{ ref('terra_dbt__msg_events')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at >= (select max(system_created_at) from {{source('silver_terra', 'msg_events')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id, msg_index, event_index, event_type order by system_created_at desc)) = 1
