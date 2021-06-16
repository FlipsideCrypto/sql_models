{{ 
  config(
    materialized='incremental', 
    sort=['block_timestamp', 'block_id'], 
    unique_key='chain_id || tx_id || msg_index || event_index', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver', 'msgs']
  )
}}

{% set BRONZE_BACKFILL_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DS_BF_TERRA_MSG_EVENT_MODEL_V2021_06_15_0_PROD_1133065308"' %}
{% set BRONZE_TABLES = [BRONZE_BACKFILL_TABLE] %}

{{ 
  bronze_kafka_extract(
    BRONZE_TABLES,
    "
      t.value:blockchain::string as blockchain,
      t.value:block_id::bigint as block_id,
      t.value:block_timestamp::timestamp as block_timestamp,
      t.value:chain_id::string as chain_id,
      t.value:tx_id::string as tx_id,
      t.value:tx_module::string as tx_module,
      t.value:tx_status::string as tx_status,
      t.value:tx_type::string as tx_type,
      t.value:msg_index::integer as msg_index,
      t.value:msg_type::string as msg_type,
      t.value:msg_module::string as msg_module,
      t.value:event_type::string as event_type,
      t.value:event_index::integer as event_index,
      t.value:event_attributes::object as event_attributes
    ",
    "chain_id, tx_id, msg_index, event_index"
  )
}}
