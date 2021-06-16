{{ 
  config(
    materialized='incremental', 
    sort=['block_timestamp', 'block_id'], 
    unique_key='chain_id || block_id || proposer_address', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'block_id'],
    tags=['snowflake', 'terra_silver', 'blocks']
  )
}}

{% set BRONZE_BACKFILL_TABLE = '"FLIPSIDE_PROD_DB"."BRONZE"."DS_BF_TERRA_BLOCK_MODEL_V2021_06_15_0_PROD_1027559933"' %}
{% set BRONZE_TABLES = [BRONZE_BACKFILL_TABLE] %}

{{ 
  bronze_kafka_extract(
    BRONZE_TABLES,
    "
      t.value:blockchain::string as blockchain,
      t.value:block_id::bigint as block_id,
      t.value:block_timestamp::timestamp as block_timestamp,
      t.value:chain_id::string as chain_id,
      t.value:proposer_address::string as proposer_address
    ",
    "chain_id, block_id, proposer_address"
  )
}}
