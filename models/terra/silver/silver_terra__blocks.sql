{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver_2', 'terra_blocks']
  )
}}

select *
from {{ ref('terra_dbt__blocks')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_terra', 'blocks')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id order by system_created_at desc)) = 1
