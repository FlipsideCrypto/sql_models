{{ 
  config(
    materialized='incremental', 
    sort=['block_timestamp', 'block_id'], 
    unique_key='chain_id || index || transition_type || event', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver', 'transitions']
  )
}}

{% set BRONZE_BACKFILL_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DS_BF_TERRA_TRANSITION_MODEL_V2021_06_15_0_PROD_680478501"' %}
{% set BRONZE_TABLES = [BRONZE_BACKFILL_TABLE] %}

{{ 
  bronze_kafka_extract(
    BRONZE_TABLES,
    "
      t.value:blockchain::string as blockchain,
      t.value:block_id::bigint as block_id,
      t.value:block_timestamp::timestamp as block_timestamp,
      t.value:chain_id::string as chain_id,
      t.value:event::string as event,
      t.value:index::integer as index,
      t.value:transition_type::string as transition_type,
      t.value:attributes::object as event_attributes
    ",
    "chain_id, event, index, transition_type"
  )
}}
