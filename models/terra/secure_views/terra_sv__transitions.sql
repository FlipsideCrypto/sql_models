{{ config(
  materialized = 'view',
  secure = 'true',
  tags = ['snowflake', 'terra_views', 'transitions', 'terra', 'secure_views']
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  transition_type,
  INDEX,
  event,
  event_attributes
FROM
  {{ ref('silver_terra__transitions') }}
