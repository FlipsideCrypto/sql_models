{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra_views', 'transitions', 'terra']
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
  event_attributes
FROM {{source('silver_terra', 'transitions')}}