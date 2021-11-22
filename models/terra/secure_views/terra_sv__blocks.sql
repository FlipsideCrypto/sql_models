{{ config(
  materialized = 'view',
  secure = 'true',
  tags = ['snowflake', 'terra_views', 'blocks', 'terra', 'secure_views'],
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  proposer_address
FROM
  {{ ref('silver_terra__blocks') }}
