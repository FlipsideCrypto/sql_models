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
  attributes:validator::string AS validator_address,
  attributes:amount:denom::string AS attribute_currency,
  attributes:amount:amount AS attribute_amount
FROM {{source('terra', 'terra_transitions')}}