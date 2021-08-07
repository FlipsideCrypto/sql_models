{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id || msg_index', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id', 'tx_id'],
    tags=['snowflake', 'terra_silver_2', 'terra_msgs']
  )
}}

select *
from {{ ref('terra_dbt__msgs')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at >= (select max(system_created_at) from {{source('silver_terra', 'msgs')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id, msg_index order by system_created_at desc)) = 1
