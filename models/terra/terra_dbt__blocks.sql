{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'terra_silver', 'terra_blocks']
  )
}}

with base_tables as (
  select *
  from {{source('bronze', 'prod_terra_sink_645110886')}}
  where record_content:model:name::string = 'terra_block_model'
  {% if is_incremental() %}
        AND (record_metadata:CreateTime::int/1000)::timestamp::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('terra_dbt', 'blocks')}})
  {% endif %}
  )

select (record_metadata:CreateTime::int/1000)::timestamp as system_created_at
, record_content:model:blockchain::string as chain_id
, t.value:block_id::int as block_id
, t.value:block_timestamp::timestamp as block_timestamp
, t.value:blockchain::string as blockchain
, t.value:proposer_address::string as proposer_address
from base_tables
,lateral flatten(input => record_content:results) t