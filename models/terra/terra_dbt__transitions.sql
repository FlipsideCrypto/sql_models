{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
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
  attributes:amount:denom::string AS currency,
  attributes:amount:amount AS amount
FROM {{source('terra', 'terra_transitions')}}
WHERE
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '1 days'
{% else %}
  block_timestamp >= getdate() - interval '9 months'
{% endif %}