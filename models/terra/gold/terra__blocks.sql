{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'blocks', 'terra'],
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  proposer_address,
  tx_count
FROM
  {{ ref('silver_terra__blocks') }}
