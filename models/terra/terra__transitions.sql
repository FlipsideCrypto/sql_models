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
FROM {{source('terra', 'terra_transitions')}}
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}