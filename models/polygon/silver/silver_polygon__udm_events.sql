{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id || log_index', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'polygon_silver', 'polygon_udm_events','polygon']
  )
}}

select *
from (
select *
from {{ ref('polygon_dbt__udm_events')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_polygon', 'udm_events')}})
{% endif %}
QUALIFY(rank() over(partition by tx_id order by block_id desc)) = 1
)
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id, log_index order by system_created_at desc)) = 1