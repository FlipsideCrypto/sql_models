{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'blocks']
  )
}}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  proposer_address
FROM {{source('terra', 'terra_blocks')}}
