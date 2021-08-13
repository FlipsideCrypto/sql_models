{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra_views', 'blocks', 'terra'],
  )
}}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  proposer_address
FROM {{source('silver_terra', 'blocks')}}
-- WHERE
-- {% if is_incremental() %}
--   block_timestamp >= getdate() - interval '1 days'
-- {% else %}
--   block_timestamp >= getdate() - interval '9 months'
-- {% endif %}