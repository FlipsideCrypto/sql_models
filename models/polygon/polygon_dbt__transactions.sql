{{ 
  config(
    materialized='incremental',
    unique_key='chain_id || block_id || tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'polygon_silver', 'polygon_dbt_transactions']
  )
}}

with base_tables as (
  select *
  from {{source('bronze', 'prod_matic_sink_510901820')}}
  where record_content:model:name::string = 'polygon_txs_model'
  {% if is_incremental() %}
        AND (record_metadata:CreateTime::int/1000)::timestamp >= (select max(system_created_at) from {{source('polygon_dbt', 'transactions')}})
  {% endif %}
  )

select (record_metadata:CreateTime::int/1000)::timestamp as system_created_at
, record_content:model:blockchain::string as chain_id
, a.value:block_id::int as block_id
, a.value:block_timestamp::timestamp as block_timestamp
, a.value:fee::float as fee
, a.value:from_address::string as from_address
, a.value:gas_limit::int as gas_limit
, a.value:gas_price::int as gas_price
, a.value:gas_used::int as gas_used
, a.value:input_method::string as input_method
, a.value:native_value::float as native_value
, a.value:nonce::int as nonce
, a.value:success::boolean as success
, a.value:to_address::string as to_address
, a.value:tx_id::string as tx_id
, a.value:tx_position::int as tx_position
from base_tables
,lateral flatten(input => record_content:results) a