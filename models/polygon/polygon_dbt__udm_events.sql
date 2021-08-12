{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id || log_index', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'polygon_silver', 'polygon_dbt_udm_events','polygon']
  )
}}

with base_tables as (
  select *
  from {{source('bronze', 'prod_matic_sink_510901820')}}
  where record_content:model:name::string = 'polygon_udm_events_model'
  {% if is_incremental() %}
        AND (record_metadata:CreateTime::int/1000)::timestamp::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{source('polygon_dbt', 'udm_events')}})
  {% else %}
    AND (record_metadata:CreateTime::int/1000)::timestamp >= '2021-08-10 10:27:18.676'
  {% endif %}
  )

select (record_metadata:CreateTime::int/1000)::timestamp as system_created_at
, record_content:model:blockchain::string as chain_id
, a.value:block_id::int as block_id
, a.value:block_timestamp::timestamp as block_timestamp
, a.value:contract_address::string as contract_address
, a.value:fee::float as fee
, a.value:from_address::string as from_address
, a.value:input_method::string as input_method
, a.value:log_index::int as log_index
, a.value:log_method::string as log_method
, a.value:name::string as name
, a.value:native_value::float as native_value
, a.value:symbol::string as symbol
, a.value:to_address::string as to_address
, a.value:token_value::float as token_value
, a.value:tx_id::string as tx_id
from base_tables
,lateral flatten(input => record_content:results) a
