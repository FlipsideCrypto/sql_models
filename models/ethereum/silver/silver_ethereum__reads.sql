{{ 
  config(
    materialized='incremental',
    unique_key='block_id || contract_address || function_name', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'ethereum_silver', 'ethereum_reads','ethereum']
  )
}}

select *
from {{ ref('ethereum_dbt__reads')}}
WHERE 1=1
{% if is_incremental() %}
        AND system_created_at::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('silver_ethereum', 'reads')}})
{% endif %}
QUALIFY(row_number() over(partition by block_id, contract_address, function_name order by system_created_at desc)) = 1