{{ 
  config(
    materialized='incremental',
    unique_key="CONCAT('-', chain_id, block_id, tx_id)", 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'polygon_silver', 'polygon_transactions','polygon']
  )
}}

select *
from {{ ref('polygon_dbt__transactions')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at >= (select dateadd('day',-1,max(system_created_at::date)) from {{ this }})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, tx_id order by block_id desc, system_created_at desc)) = 1