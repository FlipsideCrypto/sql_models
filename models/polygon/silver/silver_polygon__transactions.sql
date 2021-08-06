{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'polygon_silver', 'polygon_transactions']
  )
}}

select *
from {{ ref('polygon_dbt__transactions')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at >= (select max(system_created_at) from {{source('silver_polygon', 'transactions')}})
{% endif %}
QUALIFY(row_number() over(partition by chain_id, block_id, tx_id order by system_created_at desc)) = 1