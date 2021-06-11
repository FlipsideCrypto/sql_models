{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'validator_voting_power']
  )
}}

SELECT
  block_number AS block_id, 
  block_timestamp,
  blockchain,
  address,
  voting_power
FROM {{source('terra', 'terra_validator_voting_power')}}
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}