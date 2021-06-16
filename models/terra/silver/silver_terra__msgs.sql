{{ 
  config(
    materialized='incremental', 
    sort=['block_timestamp', 'block_id'], 
    unique_key='chain_id || tx_id || msg_index', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver', 'msgs']
  )
}}

{% set BRONZE_BACKFILL_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DS_BF_TERRA_MSG_MODEL_V2021_06_15_0_PROD_246303625"' %}
-- {% set BRONZE_REALTIME_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DOOR_SINK_TERRA_MSG_MODEL_PROD_78141091"' %}
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
      t.value:tx_type::string as tx_type,
      t.value:tx_status::string as tx_status,
      t.value:tx_module::string as tx_module,
      t.value:msg_index::integer as msg_index,
      t.value:msg_type::string as msg_type,
      t.value:msg_module::string as msg_module,
      t.value:msg_value::variant as msg_value
    ",
    "chain_id, tx_id, msg_index"
  )
}}
