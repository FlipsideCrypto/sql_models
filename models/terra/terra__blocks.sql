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
WHERE

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}