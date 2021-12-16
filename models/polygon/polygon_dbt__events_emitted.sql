{{ 
  config(
    materialized='incremental',
    unique_key="CONCAT_WS('-', chain_id, block_id, tx_id, coalesce(event_index,-1))", 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'polygon_silver', 'polygon_dbt_events_emitted','polygon']
  )
}}

with base_tables as (
  select *
  from {{source('bronze', 'prod_matic_sink_510901820')}}
  where record_content:model:name::string = 'polygon_events_emitted_model'
  {% if is_incremental() %}
        AND (record_metadata:CreateTime::int/1000)::timestamp::date >= (select dateadd('day',-1,max(system_created_at::date)) from {{ this }})
  {% endif %}
  )

select (record_metadata:CreateTime::int/1000)::timestamp as system_created_at
, record_content:model:blockchain::string as chain_id
, a.value:block_id::int as block_id
, a.value:block_timestamp::timestamp as block_timestamp
, a.value:contract_address::string as contract_address
, a.value:contract_name::string as contract_name
, a.value:event_index::int as event_index
, a.value:event_inputs::object as event_inputs
, a.value:event_name::string as event_name
, a.value:event_removed::boolean as event_removed
, a.value:tx_from::string as tx_from
, a.value:tx_id::string as tx_id
, a.value:tx_succeeded::boolean as tx_succeeded
, a.value:tx_to::string as tx_to
from base_tables
,lateral flatten(input => record_content:results) a