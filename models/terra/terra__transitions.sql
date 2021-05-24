{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'transitions']
  )
}}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  transition_type,
  index,
  event,
  attributes
FROM {{source('terra', 'terra_transitions')}}