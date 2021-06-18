{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra_views', 'transitions']
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
-- WHERE
-- {% if is_incremental() %}
--   block_timestamp >= getdate() - interval '1 days'
-- {% else %}
--   block_timestamp >= getdate() - interval '9 months'
-- {% endif %}