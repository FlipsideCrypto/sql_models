{{ 
  config(
    materialized='incremental',
    unique_key='block_id || contract_address || function_name || inputs', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum_silver', 'ethereum_dbt_reads','ethereum']
  )
}}

with base_tables as (
  select *
  from {{source('bronze', 'prod_ethereum_sink_407559501')}}
  where record_content:model:name::string like 'ethereum-user-generated%'
  {% if is_incremental() %}
        AND (record_metadata:CreateTime::int/1000)::timestamp::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('ethereum_dbt', 'reads')}})
  {% endif %}
  )

select (record_metadata:CreateTime::int/1000)::timestamp as system_created_at
, record_content:model:blockchain::string as chain_id
, a.value:block_id::int as block_id
, a.value:block_timestamp::timestamp as block_timestamp
, a.value:contract_address::string as contract_address
, a.value:contract_name::string as contract_name
, a.value:function_name::string as function_name
, a.value:inputs::object as inputs
, a.value:project_id::string as project_id
, a.value:project_name::string as project_name
, a.value:value_numeric::float as value_numeric
, a.value:value_object::object as value_object
, a.value:value_string::string as value_string
from base_tables
,lateral flatten(input => record_content:results) a