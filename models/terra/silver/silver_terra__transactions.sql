{{ 
  config(
    materialized='incremental', 
    sort=['block_timestamp', 'block_id'], 
    unique_key='chain_id || tx_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver', 'transactions']
  )
}}

{% set BRONZE_BACKFILL_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DS_BF_TERRA_TX_MODEL_V2021_06_15_0_PROD_174292778"' %}
-- {% set BRONZE_REALTIME_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DOOR_SINK_TERRA_TX_MODEL_PROD_1210790700"' %}
{% set BRONZE_TABLES = [BRONZE_BACKFILL_TABLE] %}

{{ 
  bronze_kafka_extract(
    BRONZE_TABLES,
    "
      t.value:blockchain::string as blockchain,
      t.value:block_id::bigint as block_id,
      t.value:block_timestamp::timestamp as block_timestamp,
      t.value:chain_id::string as chain_id,
      t.value:codespace::string as codespace,
      t.value:tx_id::string as tx_id,
      t.value:tx_type::string as tx_type,
      t.value:tx_module::string as tx_module,
      t.value:tx_status::string as tx_status,
      t.value:tx_status_msg::string as tx_status_msg,
      t.value:tx_code::integer as tx_code,
      t.value:fee::array as fee,
      t.value:gas_wanted::double as gas_wanted,
      t.value:gas_used::double as gas_used
    ",
    "chain_id, tx_id"
  )
}}
