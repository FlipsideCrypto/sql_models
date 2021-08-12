{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || index || transition_type || event', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'terra_silver', 'terra_transitions']
  )
}}

with base_tables as (
  select *
  from {{source('bronze', 'prod_terra_sink_645110886')}}
  where record_content:model:name::string = 'terra_transition_model'
  {% if is_incremental() %}
        AND (record_metadata:CreateTime::int/1000)::timestamp::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('terra_dbt', 'transitions')}})
  {% endif %}
  )

select (record_metadata:CreateTime::int/1000)::timestamp as system_created_at,
      t.value:blockchain::string as blockchain,
      t.value:block_id::bigint as block_id,
      t.value:block_timestamp::timestamp as block_timestamp,
      t.value:chain_id::string as chain_id,
      t.value:event::string as event,
      t.value:index::integer as index,
      t.value:transition_type::string as transition_type,
      t.value:attributes::object as event_attributes
from base_tables
,lateral flatten(input => record_content:results) t