{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'transitions', 'terra']
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
